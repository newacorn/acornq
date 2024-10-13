-- KEYS[1] -> asynq:{queueName}:success or asynq:{queueName}:failed
-- KEYS[2] -> asynq:{queueName}:active
-- KEYS[3..n] -> asynq:{queueName}:t:taskID
-- ARGV[2..n] -> task retention
local archive = KEYS[1]
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
return redis.status_reply("OK")