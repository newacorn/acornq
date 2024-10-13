--- KEYS[1] -> asynq:{queueName}:pending
--- KEYS[2..n] -> asynq:{queueName}:t:taskID
--- ARGV[1..n] -> task json encoded data
---
local pending = KEYS[1]
for i=1, #ARGV do
    redis.call('json.set', KEYS[i+1],'$', ARGV[i])
    redis.call('LPUSH',pending, KEYS[i+1])
end
return redis.status_reply("OK")
