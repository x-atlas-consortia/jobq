import subprocess
import time
import os
from redis import Redis

NUM_WORKERS = 100
WORKER_SCRIPT = "custom_worker.py"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

JOB_QUEUE_KEY = "job_queue_zset"
WORKER_COUNT_KEY = "desired_worker_count"
SHUTDOWN_FLAG_KEY = "worker_shutdown_flag"

MIN_WORKERS = 5
MAX_WORKERS = 100
INCREMENT = 5
POLL_INTERVAL = 5  

redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

processes = []

def spawn_workers(n):
    for _ in range(n):
        proc = subprocess.Popen(["python3", WORKER_SCRIPT])
        processes.append(proc)

def current_queue_size():
    return redis_conn.zcard(JOB_QUEUE_KEY)

def update_desired_count(count):
    redis_conn.set(WORKER_COUNT_KEY, count)

def get_desired_count():
    val = redis_conn.get(WORKER_COUNT_KEY)
    return int(val) if val else 0

def set_shutdown_flag(flag):
    redis_conn.set(SHUTDOWN_FLAG_KEY, int(flag))

try:
    update_desired_count(MIN_WORKERS)
    set_shutdown_flag(0)

    while True:
        queue_size = current_queue_size()
        current_workers = len(processes)
        desired_workers = get_desired_count()

        if queue_size > current_workers + INCREMENT and desired_workers < MAX_WORKERS:
            increment = min(INCREMENT, MAX_WORKERS - desired_workers)
            spawn_workers(increment)
            desired_workers += increment
            update_desired_count(desired_workers)
            set_shutdown_flag(0)
            print(f"Ramped up: added {increment} workers (desired {desired_workers})")

        elif queue_size < desired_workers - INCREMENT and desired_workers > MIN_WORKERS:
            decrement = min(INCREMENT, desired_workers - MIN_WORKERS)
            desired_workers -= decrement
            update_desired_count(desired_workers)
            set_shutdown_flag(1)
            print(f"Ramped down: target workers decreased by {decrement} (desire {desired_workers})")
        
        for proc in processes[:]:
            if proc.poll() is not None:
                processes.remove(proc)
        
        time.sleep(POLL_INTERVAL)
except KeyboardInterrupt:
    print("Terminating all workers...")
    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.wait()
    print("All workers terminated.")


# try:
#     for i in range(NUM_WORKERS):
#         print(f"Starting worker {i+1}/{NUM_WORKERS}")
#         proc = subprocess.Popen(["python3", WORKER_SCRIPT])
#         processes.append(proc)
#         time.sleep(0.05)  # optional: small delay to avoid flooding the OS

#     print(f"All {NUM_WORKERS} workers started. Press Ctrl+C to terminate.")

#     for proc in processes:
#         proc.wait()

# except KeyboardInterrupt:
#     print("Terminating all workers...")
#     for proc in processes:
#         proc.terminate()
#     for proc in processes:
#         proc.wait()
#     print("All workers terminated.")