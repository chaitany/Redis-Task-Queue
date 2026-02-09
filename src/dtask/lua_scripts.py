"""Lua scripts for atomic Redis operations."""

PROMOTE_SCHEDULED_SCRIPT = """
local scheduled_key = KEYS[1]
local task_hash = KEYS[2]
local now = tonumber(ARGV[1])
local queue_prefix = ARGV[2]

local ready = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now)
local count = 0

for _, task_id in ipairs(ready) do
    local raw = redis.call('HGET', task_hash, task_id)
    redis.call('ZREM', scheduled_key, task_id)
    if raw then
        local task = cjson.decode(raw)
        if task.state == 'pending' or task.state == 'retrying' then
            task.state = 'queued'
            task.updated_at = now
            redis.call('HSET', task_hash, task_id, cjson.encode(task))
            redis.call('LPUSH', queue_prefix .. ':' .. task.queue, task_id)
            count = count + 1
        end
    end
end

return count
"""

CLAIM_TASK_SCRIPT = """
local task_hash = KEYS[1]
local processing_key = KEYS[2]
local task_id = ARGV[1]
local worker_id = ARGV[2]
local now = tonumber(ARGV[3])

local raw = redis.call('HGET', task_hash, task_id)
if not raw then
    return 0
end

local task = cjson.decode(raw)
if task.state ~= 'queued' and task.state ~= 'retrying' then
    return 0
end

task.state = 'running'
task.worker_id = worker_id
task.started_at = now
task.attempts = task.attempts + 1
task.updated_at = now
redis.call('HSET', task_hash, task_id, cjson.encode(task))
redis.call('LPUSH', processing_key, task_id)
return 1
"""

COMPLETE_TASK_SCRIPT = """
local task_hash = KEYS[1]
local processing_key = KEYS[2]
local task_id = ARGV[1]
local result_json = ARGV[2]
local now = tonumber(ARGV[3])

local raw = redis.call('HGET', task_hash, task_id)
if not raw then
    redis.call('LREM', processing_key, 1, task_id)
    return 0
end

local task = cjson.decode(raw)
task.state = 'success'
task.result = cjson.decode(result_json)
task.completed_at = now
task.error = cjson.null
task.updated_at = now
redis.call('HSET', task_hash, task_id, cjson.encode(task))
redis.call('LREM', processing_key, 1, task_id)
return 1
"""

FAIL_TASK_SCRIPT = """
local task_hash = KEYS[1]
local processing_key = KEYS[2]
local scheduled_key = KEYS[3]
local dead_set = KEYS[4]
local task_id = ARGV[1]
local error_msg = ARGV[2]
local now = tonumber(ARGV[3])
local max_retries = tonumber(ARGV[4])
local retry_delay = tonumber(ARGV[5])
local attempts = tonumber(ARGV[6])

local raw = redis.call('HGET', task_hash, task_id)
if not raw then
    redis.call('LREM', processing_key, 1, task_id)
    return 'gone'
end

local task = cjson.decode(raw)
task.error = error_msg
task.updated_at = now

if attempts <= max_retries then
    task.state = 'retrying'
    local delay = retry_delay * math.pow(2, attempts - 1)
    redis.call('HSET', task_hash, task_id, cjson.encode(task))
    redis.call('ZADD', scheduled_key, now + delay, task_id)
    redis.call('LREM', processing_key, 1, task_id)
    return 'retry'
else
    task.state = 'dead'
    task.completed_at = now
    redis.call('HSET', task_hash, task_id, cjson.encode(task))
    redis.call('SADD', dead_set, task_id)
    redis.call('LREM', processing_key, 1, task_id)
    return 'dead'
end
"""

REQUEUE_ORPHAN_SCRIPT = """
local task_hash = KEYS[1]
local queue_prefix = ARGV[1]
local task_id = ARGV[2]

local raw = redis.call('HGET', task_hash, task_id)
if not raw then
    return 0
end

local task = cjson.decode(raw)
if task.state == 'success' or task.state == 'dead' then
    return 0
end

task.state = 'queued'
task.worker_id = cjson.null
task.updated_at = tonumber(ARGV[3])
redis.call('HSET', task_hash, task_id, cjson.encode(task))
redis.call('LPUSH', queue_prefix .. ':' .. task.queue, task_id)
return 1
"""

CANCEL_TASK_SCRIPT = """
local task_hash = KEYS[1]
local scheduled_key = KEYS[2]
local task_id = ARGV[1]
local queue_prefix = ARGV[2]
local now = tonumber(ARGV[3])

local raw = redis.call('HGET', task_hash, task_id)
if not raw then
    return 0
end

local task = cjson.decode(raw)
if task.state ~= 'pending' and task.state ~= 'queued' then
    return 0
end

local queue_key = queue_prefix .. ':' .. task.queue
redis.call('LREM', queue_key, 0, task_id)
redis.call('ZREM', scheduled_key, task_id)

task.state = 'dead'
task.error = 'Cancelled by user'
task.updated_at = now
redis.call('HSET', task_hash, task_id, cjson.encode(task))
return 1
"""
