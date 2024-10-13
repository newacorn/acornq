package acornq

import "github.com/redis/rueidis"

var (
	cleanerLs          = rueidis.NewLuaScript(cleanerLuaScript)
	recoveryLs         = rueidis.NewLuaScript(recoveryTasksLuaScript)
	pickTasksLs        = rueidis.NewLuaScript(pickTasksLuaScript)
	retryTasksLs       = rueidis.NewLuaScript(retryTasksLuaScript)
	active2pendingLs   = rueidis.NewLuaScript(active2pendingLuaScript)
	active2ArchiveLs   = rueidis.NewLuaScript(active2ArchiveLuaScript)
	enqueuePendingLs   = rueidis.NewLuaScript(enqueuePendingLuaScript)
	enqueueScheduledLs = rueidis.NewLuaScript(enqueueScheduledLuaScript)
)

// --- KEYS[1] -> asynq:{queueName}:pending
// --- KEYS[2..n] -> asynq:{queueName}:t:taskID
// --- ARGV[1..n] -> task json encoded data
// ---
var enqueuePendingLuaScript =
// lang=lua
`local pending = KEYS[1]
for i=1, #ARGV do
    redis.call('json.set', KEYS[i+1],'$', ARGV[i])
    redis.call('LPUSH',pending, KEYS[i+1])
end
return redis.status_reply("OK")`

// --- KEYS[1] -> asynq:{queueName}:scheduled
// --- KEYS[2..n] -> asynq:{queueName}:t:taskID
// --- ARGV[1..n] -> task json encoded data
// ---
var enqueueScheduledLuaScript = `local scheduled = KEYS[1]
for i=1, #ARGV do
    redis.call('json.set', KEYS[i+1],'$', ARGV[i])
    local  startAt = string.match(ARGV[i],'"start_at":%s*(%d+)')
    if startAt then
        redis.call('ZADD', scheduled, startAt, KEYS[i+1])
    end
end
return redis.status_reply("OK")`

// --- KEYS[1] -> asynq:{queueName}:active
// --- KEYS[2] -> asynq:{queueName}:live
// --- KEYS[3] -> asynq:{queueName}:pending
// --- ARGV[1] -> task idle duration in seconds
// --- ARGV[2] -> pending state
// ---
var recoveryTasksLuaScript = `local function pendingAt(task,active)
    local resp = redis.call("JSON.GET",task,"$.pending_at")
     if resp then
         if string.len(resp)>2 then
             return string.sub(resp, 2, string.len(resp)-1)
         end
     else
         redis.call("LREM",active,1,task)
     end
end
local function toActiveTable(l,active)
    local result = {}
    for i=1, #l do
        local score = pendingAt(l[i],active)
        if score then
            result[l[i]] = score
        end
    end
    return result
end
local function set2Table(s)
    local result = {}
    for i=1, #s, 2 do
        result[s[i]] = s[i+1]
    end
    return result
end
local function deleteZombieActive(set1, set2,timeout,now)
    local result = {}
    for key in pairs(set1) do
        if not set2[key] then
            if now - tonumber(set1[key]) > timeout then
                table.insert(result, key)
            end
        end
    end
    return result
end
local function deleteZombieLive(set1, set2)
    local result = {}
    for key in pairs(set1) do
        if not set2[key] then
            table.insert(result, key)
        end
    end
    return result
end
local function deleteIdleActive(set1,set2, timeout,now)
    local result = {}
    for key in pairs(set1) do
        if set2[key] then
            local score1 = tonumber(set1[key])
            local score2 = tonumber(set2[key])
            local score = score2
            if score1 > score2 then
                score = score1
            end
            if now -score > timeout then
                table.insert(result, key)
            end
        end
    end
    return result
end
local active = KEYS[1]
local live = KEYS[2]
local pending = KEYS[3]
local duration = tonumber(ARGV[1])
local pendingState = ARGV[2]
local liveSet = redis.call("ZRANGE",live,0,-1,"WITHSCORES")
local activeList = redis.call("LRANGE",active,0,-1)
if #activeList > 0 then
    local now = tonumber(redis.call("TIME")[1])
    local activeTable = toActiveTable(activeList,active)
    local liveTable = set2Table(liveSet)
    --- task in active list not in live set and idle more than duration
    local del1 = deleteZombieActive(activeTable,liveTable,duration,now)
    --- task in active and live, but idle more than duration
    local del2 = deleteIdleActive(activeTable,liveTable,duration,now)
    if #del1 > 0 then
        redis.call("LPUSH",pending, unpack(del1))
        for i=1, #del1 do
            redis.call("JSON.MSET",del1[i],"$.state",pendingState,del1[i],"$.pending_at",now)
            redis.call("LREM",active,1,del1[i])
        end
    end
    if #del2 > 0 then
        redis.call("LPUSH",pending, unpack(del2))
        for i=1, #del1 do
            redis.call("JSON.MSET",del2[i],"$.state",pendingState,del2[i],"$.pending_at",now)
            redis.call("LREM",active,1,del2[i])
        end
        redis.call("SREM",live, unpack(del2))
    end
    local del3 = deleteZombieLive(liveTable,activeTable)
    if #del3 > 0 then
        redis.call("SREM",live, unpack(del3))
    end
    return redis.status_reply("OK")
end
redis.call("DEL",live)
return redis.status_reply("OK")`

// --// PickTasks from pending set.
// --// 1. move task from scheduled list to pending list.
// --// 2. move task from retry list to pending list.
// --// 3. move task from pending list to active list and return them.
//
// --- KEYS[1] -> asynq:{queueName}:pending
// --- KEYS[2] -> asynq:{queueName}:active
// --- KEYS[3] -> asynq:{queueName}:scheduled
// --- KEYS[4] -> asynq:{queueName}:retry
// --- ARGV[1] -> task count
// --- ARGV[2] -> pending state
// --- ARGV[3] -> active state
// ---
var pickTasksLuaScript = `local pending = KEYS[1]
local active = KEYS[2]
local scheduled = KEYS[3]
local retry = KEYS[4]
local count = tonumber(ARGV[1])
local now = tonumber(redis.call("TIME")[1])
local pendingState = ARGV[2]
local activeState = ARGV[3]

local move1=redis.call("ZRANGEBYSCORE",scheduled,0,now)
if #move1 > 0 then
    redis.call("LPUSH",pending, unpack(move1))
    for i=1, #move1 do
        --redis.call("json.set",move1[i],"$.pending_at",now)
        redis.call("JSON.MSET",move1[i],"$.pending_at",now,move1[i],"$.state",pendingState)
    end
    redis.call("ZREM",scheduled, unpack(move1))
end
local move2=redis.call("ZRANGEBYSCORE",retry,0,now)
if #move2 > 0 then
    redis.call("LPUSH",pending, unpack(move2))
    for i=1, #move2 do
        --redis.call("json.set",move2[i],"$.pending_at",now)
        redis.call("JSON.MSET",move2[i],"$.pending_at",now,move2[i],"$.state",pendingState)
    end
    redis.call("ZREM",retry, unpack(move2))
end
local result ={}
for _=1,count do
    local taskKey = redis.call("RPOPLPUSH",pending,active)
    if not taskKey then
        return result
    end
    redis.call("JSON.MSET",taskKey,"$.pending_at",now,taskKey,"$.state",activeState)
    local task = redis.call("json.get",taskKey)
    if task then
        table.insert(result, task)
    end
end
return result`

// -- RetryTasks remove ts from active list and add tasks to retry sorted set conditional.
// -- KEYS[1] -> asynq:{queueName}:retry
// -- KEYS[2] -> asynq:{queueName}:active
// -- KEYS[3..n] -> asynq:{queueName}:t:taskID
// -- ARGV[1] -> retry state
// -- ARGV[2n] -> task start at unix timestamp seconds
// -- ARGV[2n+1] -> retried count
var retryTasksLuaScript = `local retry = KEYS[1]
local active = KEYS[2]
local retryState = ARGV[1]
local j = 2
for i = 3, #KEYS do
    local taskKey = KEYS[i]
    local score = tonumber(ARGV[j])
    local retriedCount = tonumber(ARGV[j + 1])
    j = j + 2
    redis.call("ZADD", retry, score, taskKey)
    redis.call("JSON.MSET", taskKey, "$.state", retryState, taskKey, "$.retried", retriedCount)
    redis.call("LREM", active, 1, taskKey)
end
return redis.status_reply("OK")`

// -- KEYS[1] -> asynq:{queueName}:pending
// -- KEYS[2] -> asynq:{queueName}:active
// -- KEYS[3..n] -> asynq:{queueName}:t:taskID
var active2pendingLuaScript = `local pending = KEYS[1]
local active = KEYS[2]
local now = tonumber(redis.call("TIME")[1])

redis.call('LPUSH', pending, unpack(KEYS, 3, #KEYS))
for i=3, #KEYS do
        redis.call('LREM', active, 1, KEYS[i])
        redis.call('json.set', KEYS[i], '$.pending_at', now)
end
return redis.status_reply("OK")`

// -- KEYS[1] -> asynq:{queueName}:success or asynq:{queueName}:failed
// -- KEYS[2] -> asynq:{queueName}:active
// -- KEYS[3..n] -> asynq:{queueName}:t:taskID
// -- ARGV[2..n] -> task retention
var active2ArchiveLuaScript = `local archive = KEYS[1]
local active = KEYS[2]
local todel = KEYS[3]
local state = ARGV[1]
local now = tonumber(redis.call("TIME")[1])

for i = 2, #ARGV do
    local taskKey = KEYS[i + 2]
    local retention = tonumber(ARGV[i])
    if retention~=0 then
        redis.call('LPUSH', archive, taskKey)
        if retention>0 then
            redis.call('EXPIRE', taskKey, retention)
            redis.call('ZADD',todel,now+retention,taskKey)
        end
        redis.call('JSON.MSET', taskKey, '$.completed_at', now, taskKey, '$.state', state)
    else
        redis.call('DEL', taskKey)
    end
    redis.call('LREM', active,1,taskKey)
end
return redis.status_reply("OK")`

// -- KEYS[3n] -> asynq:{queueName}:todel
// -- KEYS[3n+1] -> asynq:{queueName}:success
// -- KEYS[3n+2] -> asynq:{queueName}:failed
// -- ARGV[1] -> start position
// -- ARGV[2] -> end position
var cleanerLuaScript = `local startPos = tonumber(ARGV[1])
local endPos = tonumber(ARGV[2])
local now = tonumber(redis.call("TIME")[1])
local nextStartPos = 0
for i=1,3,#KEYS do
    local toDelSet = KEYS[i]
    local successList = KEYS[i+1]
    local failedList = KEYS[i+2]
    --
    if startPos==0 then
        local resp = redis.call("ZRANGEBYSCORE", toDelSet, 0, now)
        if #resp > 0 then
            for j = 1, #resp do
                local taskKey = resp[j]
                if redis.call("LREM", successList, 1, taskKey)==0 then
                    redis.call("LREM", failedList, 1, taskKey)
                end
            end
            redis.call("ZREM", toDelSet, unpack(resp))
        end
    end
    local successLen = tonumber(redis.call("LLEN", successList))
    local failedLen = tonumber(redis.call("LLEN", failedList))
    --
    if successLen > startPos then
        local resp1 = redis.call("LRANGE", successList, startPos, endPos)
        for _, taskKey in ipairs(resp1) do
            if redis.call("EXISTS", taskKey) == 0 then
                redis.call("LREM", successList, 1, taskKey)
            end
        end
        if endPos+1 < successLen then
            nextStartPos = endPos+1
        end
    end
    if failedLen > startPos then
        local resp1 = redis.call("LRANGE", failedList, startPos, endPos)
        for _, taskKey in ipairs(resp1) do
            if redis.call("EXISTS", taskKey) == 0 then
                redis.call("LREM", failedList, 1, taskKey)
            end
        end
        if endPos+1 < failedLen then
            nextStartPos = endPos+1
        end
    end
end
return nextStartPos`
