# Atlas Consortia JobQ
**Atlas Consortia JobQ** is a high-performance, Redis-backed priority queue system designed for background task management. 

## Table of Contents
* [Installation](#installation)

* [Quick Start](#quick-start)

* [Worker Management](#worker-management)

* [Method Reference](#api-reference)

* [Features](#features)


### Installation
Install the package via pip:

```Bash
pip install atlas-consortia-jobq
```
*Note: Requires a running Redis instance. Refer to the Redis [documentation](https://redis.io/docs/latest/) for instructions on installing and running Redis*

### Quick Start
**1. Initialize the Queue**
```Python
from atlas_consortia_jobq import JobQueue

# Connect to your Redis instance
jq = JobQueue(
    redis_host='localhost',
    redis_port=6379,
    redis_db=0,
    redis_password=None
)
```
**2. Enqueue a Job**

Jobs require a function, an entity_id, and optional arguments.

* job_id: A unique identifier generated for every specific job. This is created during the enqueing process and will be returned so the job may be referenced later.

* entity_id: The unique identifier of the resource being processed (e.g., a UUID). This prevents the same resource from being queued multiple times.

```Python
def my_task(arg1, keyword_arg="default"):
    print(f"Processing: {arg1}, {keyword_arg}")

job_id = jq.enqueue(
    task_func=my_task,
    entity_id="unique_id_123",
    args=["value1"],
    kwargs={"keyword_arg": "value2"},
    priority=2
)
```

### Worker Management
To process jobs, you must start worker subprocesses. This is typically done in a dedicated entry-point script.

```python
from atlas_consortia_jobq import JobQueue

if __name__ == "__main__":
    jq = JobQueue(redis_host='localhost')
    
    # This call spawns 4 worker subprocesses
    jq.start_workers(num_workers=4)
```

### Method Reference
```python
enqueue(task_func, entity_id, args=None, kwargs=None, priority=1)
```
Adds a job to the queue.

* If the entity_id is already queued, it updates the priority if the new priority is higher.

* If the entity_id is currently being processed, it prevents duplicate enqueuing.
```python
update_priority(identifier, new_priority)
```
Updates the priority of an existing job. The identifier can be a job_id or an entity_id.
```python
get_status(identifier)
```
Returns a dictionary containing the job_id, position_in_queue, and priority. Here **"identifier"** can be either the job_id or the entity_id.

```python
get_queue_status()
```
Returns an overview of the entire queue, including total job counts and a breakdown by priority level.

### Features
* Atomic Operations: Uses Lua scripting to ensure job enqueuing and popping are race-condition free.

* entity_id Deduplication: Prevents multiple jobs for the same entity_id from cluttering the queue.

* Priority Support: Supports three priority levels (1=Highest, 2=Medium, 3=Lowest).

* Automatic Cleanup: Manages metadata and "processing" states automatically upon job completion.

