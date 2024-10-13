-- KEYS[1] -> asynq:{queueName}:pending
-- KEYS[2] -> asynq:{queueName}:active
-- KEYS[3..n] -> asynq:{queueName}:t:taskID
local pending = KEYS[1]
local active = KEYS[2]
local now = tonumber(redis.call("TIME")[1])

redis.call('LPUSH', pending, unpack(KEYS, 3, #KEYS))
for i=3, #KEYS do
        redis.call('LREM', active, 1, KEYS[i])
        redis.call('json.set', KEYS[i], '$.pending_at', now)
end
return redis.status_reply("OK")