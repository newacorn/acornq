--- KEYS[1] -> asynq:{queueName}:scheduled
--- KEYS[2..n] -> asynq:{queueName}:t:taskID
--- ARGV[1..n] -> task json encoded data
---
local scheduled = KEYS[1]
for i=1, #ARGV do
    redis.call('json.set', KEYS[i+1],'$', ARGV[i])
    local  startAt = string.match(ARGV[i],'"start_at":%s*(%d+)')
    if startAt then
        redis.call('ZADD', scheduled, startAt, KEYS[i+1])
    end
end
return redis.status_reply("OK")