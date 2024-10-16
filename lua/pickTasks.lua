--// PickTasks from pending set.
--// 1. move task from scheduled list to pending list.
--// 2. move task from retry list to pending list.
--// 3. move task from pending list to active list and return them.

--- KEYS[1] -> asynq:{queueName}:pending
--- KEYS[2] -> asynq:{queueName}:active
--- KEYS[3] -> asynq:{queueName}:scheduled
--- KEYS[4] -> asynq:{queueName}:retry
--- ARGV[1] -> task count
--- ARGV[2] -> pending state
--- ARGV[3] -> active state
---
local pending = KEYS[1]
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
return result