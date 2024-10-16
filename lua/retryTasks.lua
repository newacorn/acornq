-- RetryTasks remove ts from active list and add tasks to retry sorted set conditional.
-- KEYS[1] -> asynq:{queueName}:retry
-- KEYS[2] -> asynq:{queueName}:active
-- KEYS[3..n] -> asynq:{queueName}:t:taskID
-- ARGV[1] -> retry state
-- ARGV[2n] -> task start at unix timestamp seconds
-- ARGV[2n+1] -> retried count
local retry = KEYS[1]
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
return redis.status_reply("OK")