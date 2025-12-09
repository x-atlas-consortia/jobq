import time
import json
import importlib
from redis import Redis, ConnectionError

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

JOB_QUEUE_KEY = 'job_queue_zset'
JOB_HASH_KEY = 'jobs_data'
ENTITY_INDEX_KEY = 'entity_uuid'
SHUTDOWN_COUNT_KEY = "worker_shutdown_count"

try:
    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_conn.ping()
except ConnectionError as e:
    print(f"FATAL: Worker failed to connect to Redis: {e}")
    exit(1)



POP_AND_FETCH_JOB = """
-- KEYS[1]: JOB_QUEUE_KEY (zset)
-- KEYS[2]: JOB_HASH_KEY (hash)
local pop_result = redis.call('ZPOPMIN', KEYS[1], 1)
if #pop_result == 0 then
    return nil
end
local job_id = pop_result[1] 

local job_json = redis.call('HGET', KEYS[2], job_id)

if not job_json then
    return nil 
end

return {job_id, job_json}
"""

CHECK_AND_CONSUME_SHUTDOWN_TOKEN = """
-- KEYS[1] is SHUTDOWN_COUNT_KEY
local count = redis.call('GET', KEYS[1])
if count and tonumber(count) > 0 then
    redis.call('DECR', KEYS[1])
    return 1 
end
return 0 
"""

try:
    pop_and_fetch_script = redis_conn.script_load(POP_AND_FETCH_JOB)
    shutdown_script = redis_conn.script_load(CHECK_AND_CONSUME_SHUTDOWN_TOKEN)
except Exception as e:
    print(f"FATAL: Error loading Redis scripts: {e}")
    exit(1)


def execute_job(job_data):
    module_name = job_data['task_module']
    func_name = job_data['task_name']
    args = job_data.get('args', [])
    kwargs = job_data.get('kwargs', {})

    try:
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
        func(*args, **kwargs)
        return True
    except Exception as e:
        job_id = job_data.get('job_id', 'unknown')
        print(f"Error executing job {job_id}: {e}")
        return False

print("Worker starting up.")
while True:
    try:
        result = redis_conn.evalsha(pop_and_fetch_script, 2, JOB_QUEUE_KEY, JOB_HASH_KEY)
    except Exception as e:
        print(f"FATAL: Error during job pop: {e}. Worker exiting.")
        break
    
    if not result:
        time.sleep(0.5)
        
        try:
            should_shut_down = redis_conn.evalsha(shutdown_script, 1, SHUTDOWN_COUNT_KEY)
        except Exception as e:
            print(f"FATAL: Error during shutdown check: {e}. Worker exiting.")
            break

        if should_shut_down == 1:
            print("Worker idle, consumed shutdown token, terminating.")
            break
        
        continue

    job_id = result[0]
    job_id_str = job_id.decode()
    job_json = result[1]
    job_data = json.loads(job_json)
    
    print(f"Starting job: {job_id_str} (P{job_data.get('priority', '?')})")
    
    try:
        job_successful = execute_job(job_data)
        
    finally:
        try:
            redis_conn.hdel(JOB_HASH_KEY, job_id)
            redis_conn.hdel(ENTITY_INDEX_KEY, job_data['entity_id'])
            if job_successful:
                print(f"Job {job_id_str} finished and metadata cleaned.")
            else:
                print(f"Job {job_id_str} failed. Metadata cleaned.")
        except Exception as e:
            print(f"FATAL Cleanup Error for job {job_id_str}: {e}. Worker forced to exit.")
            break
    
    try:
        should_shut_down = redis_conn.evalsha(shutdown_script, 1, SHUTDOWN_COUNT_KEY)
    except Exception as e:
        print(f"FATAL: Error during shutdown check after job: {e}. Worker exiting.")
        break

    if should_shut_down == 1:
        print(f"Worker completed job {job_id_str} and is terminating.")
        break

print("Worker shut down.")