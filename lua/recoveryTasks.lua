--- KEYS[1] -> asynq:{queueName}:active
--- KEYS[2] -> asynq:{queueName}:live
--- KEYS[3] -> asynq:{queueName}:pending
--- ARGV[1] -> task idle duration in seconds
--- ARGV[2] -> pending state
---
local function pendingAt(task,active)
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
return redis.status_reply("OK")