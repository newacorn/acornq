-- KEYS[3n] -> asynq:{queueName}:todel
-- KEYS[3n+1] -> asynq:{queueName}:success
-- KEYS[3n+2] -> asynq:{queueName}:failed
-- ARGV[1] -> start position
-- ARGV[2] -> end position
local startPos = tonumber(ARGV[1])
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
return nextStartPos