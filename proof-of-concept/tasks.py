import time
import os
from redis import Redis

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def my_task(job_priority: int):
    time.sleep(20)
    print(f"Priority: {job_priority}")
    log_entry = f"Priority: {job_priority}"
    redis_conn.rpush("job_log", log_entry)