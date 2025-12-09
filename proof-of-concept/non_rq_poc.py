from datetime import datetime, timezone
import os
import uuid
import json
from flask import Flask, request, jsonify
from redis import Redis
from tasks import my_task  

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

try:
    redis_conn = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_conn.ping()
    print(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}, DB {REDIS_DB}")
except Exception as e:
    print(f"ERROR: Could not connect to Redis. Ensure Redis server is running. {e}")

JOB_QUEUE_KEY = 'job_queue_zset'
JOB_HASH_KEY = 'jobs_data'
ENTITY_INDEX_KEY = 'entity_uuid'

app = Flask(__name__)

def generate_job_id():
    return str(uuid.uuid1())

def enqueue_job(task_func, entity_id, args=None, kwargs=None, priority=1):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    
    existing_job_id = redis_conn.hget(ENTITY_INDEX_KEY, entity_id)
    if existing_job_id:
        existing_job_json = redis_conn.hget(JOB_HASH_KEY, existing_job_id)
        if existing_job_json:
            existing_data = json.loads(existing_job_json)
            current_priority = existing_data.get("priority", None)

            if current_priority is not None and priority < current_priority:
                return apply_priority_update(existing_job_id, priority)
        return existing_job_id.decode() if isinstance(existing_job_id, bytes) else existing_job_id

    job_id = generate_job_id()
    now = datetime.now(timezone.utc).isoformat()
    job_data = {
        "task_module": task_func.__module__,
        "task_name": task_func.__name__,
        "args": args,
        "kwargs": kwargs,
        "job_id": job_id,
        "entity_id": entity_id,
        "priority": priority,
        "queued_timestamp": now,
        "priority_updated_timestamp": None
    }
    redis_conn.hset(JOB_HASH_KEY, job_id, json.dumps(job_data))
    redis_conn.zadd(JOB_QUEUE_KEY, {job_id: priority})
    redis_conn.hset(ENTITY_INDEX_KEY, entity_id, job_id)
    return job_id

@app.route('/myjob/<entityid>', methods=['GET'])
def enqueue_priority_job(entityid):
    priority = request.args.get('priority')
    if priority is None:
        priority = 1
    else:
        try:
            priority = int(priority)
        except ValueError:
            return jsonify({"error": "Priority must be an integer"}), 400
        if priority not in [1, 2, 3]:
            return jsonify({"error": "Invalid priority. Use 1, 2, or 3."}), 400
    entity_id = entityid
    job_id = enqueue_job(my_task, entity_id, args=[priority], priority=priority)

    return jsonify({
        "status": "Job enqueued successfully",
        "job_id": job_id,
        "priority": priority,
        "entity_id": entity_id
    })

@app.route('/status', methods=['GET'])
def get_queue_status():
    total_jobs = redis_conn.hlen(JOB_HASH_KEY)
    priority_counts = {
        "1": redis_conn.zcount(JOB_QUEUE_KEY, 1, 1),
        "2": redis_conn.zcount(JOB_QUEUE_KEY, 2, 2),
        "3": redis_conn.zcount(JOB_QUEUE_KEY, 3, 3)
    }
    return jsonify({
        "jobs_in_progress": total_jobs,
        "count_by_priority": priority_counts
    })

@app.route('/status/<theid>', methods=['GET'])
def status(theid):
    job_id_bytes = theid.encode() if isinstance(theid, str) else theid
    if not redis_conn.hexists(JOB_HASH_KEY, job_id_bytes):
        job_id_lookup = redis_conn.hget(ENTITY_INDEX_KEY, theid)
        if not job_id_lookup:
            return jsonify({"error": "Job not found or already completed"}), 404
        job_id_bytes = job_id_lookup

    position = redis_conn.zrank(JOB_QUEUE_KEY, job_id_bytes)
    if position is None:
        return jsonify({"error": "Job not found or already completed"}), 404

    priority = redis_conn.zscore(JOB_QUEUE_KEY, job_id_bytes)
    return jsonify({
        "job_id": job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes,
        "position_in_queue": position,
        "priority": int(priority) if priority is not None else None
    })

@app.route('/updatepriority/<theid>/<int:new_priority>', methods=['POST'])
def update_job_priority(theid, new_priority):
    raw_job = redis_conn.hget(JOB_HASH_KEY, theid)
    if raw_job:
        job_id = theid
    else:
        job_id_lookup = redis_conn.hget(ENTITY_INDEX_KEY, theid)
        if not job_id_lookup:
            return jsonify({"error": "Job not found"}), 404
        job_id = job_id_lookup

    final_id = apply_priority_update(job_id, new_priority)
    if not final_id:
        return jsonify({"error": "Job not found"}), 404

    return jsonify({"status": "success", "job_id": final_id, "new_priority": new_priority})


def apply_priority_update(job_id, new_priority):
    job_id_bytes = job_id if isinstance(job_id, bytes) else job_id.encode()
    job_json = redis_conn.hget(JOB_HASH_KEY, job_id_bytes)
    if not job_json:
        return None  

    job_data = json.loads(job_json)
    job_data['priority'] = new_priority
    job_data['priority_updated_timestamp'] = datetime.now(timezone.utc).isoformat()

    redis_conn.hset(JOB_HASH_KEY, job_id_bytes, json.dumps(job_data))
    redis_conn.zadd(JOB_QUEUE_KEY, {job_id_bytes: new_priority})

    return job_id_bytes.decode()


if __name__ == "__main__":
    app.run(debug=True)