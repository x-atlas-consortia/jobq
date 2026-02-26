import json
import uuid
import time
import importlib
import logging
import subprocess
from datetime import datetime, timezone
from typing import Callable, Any, Optional, List, Dict
from redis import Redis, ConnectionError, RedisError


class JobQueue:
    """
    A Redis-based priority queue system for managing background jobs.
    
    Uses Redis ZSET for priority ordering and hashes for job metadata and deduplication.
    Supports job enqueueing, priority updates, status checks, and worker management.
    """
    
    # Redis key constants
    JOB_QUEUE_KEY = 'job_queue_zset'
    JOB_HASH_KEY = 'jobs_data'
    ENTITY_INDEX_KEY = 'entity_uuid'
    PROCESSING_ENTITIES_KEY = 'processing_entities'
    TOTAL_PROCESSED_KEY = 'total_processed_count'
    MIN_RUNTIME_KEY = 'min_runtime_key'
    MAX_RUNTIME_KEY = 'max_runtime_key'

    
    # Lua script for atomic pop and fetch
    POP_AND_FETCH_JOB = """
    -- KEYS[1]: JOB_QUEUE_KEY (zset)
    -- KEYS[2]: JOB_HASH_KEY (hash)
    -- KEYS[3]: PROCESSING_ENTITIES_KEY (hash)
    -- ARGV[1]: current_timestamp
    local pop_result = redis.call('ZPOPMIN', KEYS[1], 1)
    if #pop_result == 0 then
        return nil
    end
    local job_id = pop_result[1]
    local job_json = redis.call('HGET', KEYS[2], job_id)
    if not job_json then
        return nil
    end
    local job_data = cjson.decode(job_json)
    local entity_id = job_data['entity_id']
    job_data['process_start_timestamp'] = ARGV[1]
    local updated_json = cjson.encode(job_data)
    redis.call('HSET', KEYS[2], job_id, updated_json)
    if entity_id then
        redis.call('HSET', KEYS[3], entity_id, job_id)
    end
    return {job_id, updated_json}
    """
    # Lua script for atomic check-and-enqueue
    ENQUEUE_JOB_ATOMIC = """
    -- KEYS[1]: JOB_QUEUE_KEY, KEYS[2]: JOB_HASH_KEY, KEYS[3]: ENTITY_INDEX_KEY, KEYS[4]: PROCESSING_ENTITIES_KEY
    -- ARGV[1]: entity_id, ARGV[2]: job_id, ARGV[3]: job_json, ARGV[4]: priority, ARGV[5]: now
    
    local existing_job_id = redis.call('HGET', KEYS[3], ARGV[1])
    if existing_job_id then
        local job_json = redis.call('HGET', KEYS[2], existing_job_id)
        if job_json then
            local job_data = cjson.decode(job_json)
            if tonumber(ARGV[4]) < tonumber(job_data['priority']) then
                job_data['priority'] = tonumber(ARGV[4])
                job_data['priority_updated_timestamp'] = ARGV[5]
                local updated_json = cjson.encode(job_data)
                redis.call('HSET', KEYS[2], existing_job_id, updated_json)
                redis.call('ZADD', KEYS[1], tonumber(ARGV[4]), existing_job_id)
                return "UPDATED:" .. existing_job_id
            end
        end
        return "EXISTS:" .. existing_job_id
    end
    
    local processing_id = redis.call('HGET', KEYS[4], ARGV[1])
    if processing_id then
        return "PROCESSING:" .. processing_id
    end
    
    redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
    redis.call('ZADD', KEYS[1], tonumber(ARGV[4]), ARGV[2])
    redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
    return "CREATED:" .. ARGV[2]
    """
    
    # Lua script for atomic compare and update
    SET_AND_COMPARE_MIN_MAX = """
    -- KEYS[1]: MIN_RUNTIME_KEY
    -- KEYS[2]: MAX_RUNTIME_KEY
    -- ARGV[1]: duration_ms

    local new_val = tonumber(ARGV[1])

    -- Handle Min
    local current_min = redis.call('get', KEYS[1])
    if not current_min or new_val < tonumber(current_min) then
        redis.call('set', KEYS[1], ARGV[1])
    end

    -- Handle Max
    local current_max = redis.call('get', KEYS[2])
    if not current_max or new_val > tonumber(current_max) then
        redis.call('set', KEYS[2], ARGV[1])
    end

    return {current_min, current_max} -- Optional: returns old values
    """

    def __init__(self, redis_host: str = 'jobq-redis', redis_port: int = 6379, 
                 redis_db: int = 0, redis_password: Optional[str] = None, log_level: int = logging.INFO):
        """
        Initialize JobQueue with Redis connection details.
        
        Args:
            redis_host: Redis server hostname
            redis_port: Redis server port
            redis_db: Redis database number
            redis_password: Redis password (optional)
            log_level: Logging level (default: logging.INFO)
            
        Raises:
            ConnectionError: If unable to connect to Redis
        """
        self.logger = logging.getLogger(f"{__name__}.JobQueue")
        self.logger.setLevel(log_level)
        self.service_start_time = time.perf_counter()

        try:
            self.redis_conn = Redis(
                host=redis_host, 
                port=redis_port, 
                db=redis_db,
                password=redis_password,
                decode_responses=False  # Keep as bytes for consistency
            )
            self.redis_conn.ping()
            self.logger.info(f"Successfully connected to Redis at {redis_host}:{redis_port}, DB {redis_db}")
            self.redis_conn.delete(self.TOTAL_PROCESSED_KEY, self.MIN_RUNTIME_KEY, self.MAX_RUNTIME_KEY, 'job_durations')
        except ConnectionError as e:
            msg = f"Failed to connect to Redis at {redis_host}:{redis_port}: {e}"
            self.logger.error(msg)
            raise ConnectionError(msg)
        except Exception as e:
            msg = f"Unexpected error while connecting to redis. {e}"
            self.logger.error(msg)
            raise ConnectionError(msg)
        
        # Load Lua script
        try:
            self.pop_and_fetch_script = self.redis_conn.script_load(self.POP_AND_FETCH_JOB)
            self.enqueue_atomic_script = self.redis_conn.script_load(self.ENQUEUE_JOB_ATOMIC)
            self.set_min_max_script = self.redis_conn.script_load(self.SET_AND_COMPARE_MIN_MAX)
            self.logger.debug("Lua scripts loaded successfully")
        except RedisError as e:
            self.logger.error(f"Failed to load Lua script: {e}")
            raise RedisError(f"Failed to load Lua script: {e}")
    
    @staticmethod
    def _generate_job_id() -> str:
        """Generate a unique chronological job ID using UUID1."""
        return str(uuid.uuid1())
    
    def enqueue(self, task_func: Callable, entity_id: str, 
                args: Optional[List] = None, kwargs: Optional[Dict] = None, 
                priority: int = 1, job_metadata: Optional[Dict] = None) -> str:
        """
        Enqueue a job for processing.
        
        If a job for this entity_id already exists:
        - If new priority is higher (lower number), update the existing job's priority
        - Otherwise, return the existing job_id without creating a duplicate
        
        Args:
            task_func: The function to be executed
            entity_id: Unique identifier for the entity being processed
            args: Positional arguments for the task function
            kwargs: Keyword arguments for the task function
            priority: Job priority (1=highest, 2=medium, 3=lowest)
            
        Returns:
            str: The job_id (either newly created or existing)
            
        Raises:
            RedisError: If Redis operations fail
            ValueError: If priority is not 1, 2, or 3
        """
        if priority not in [1, 2, 3]:
            raise ValueError(f"Invalid priority: {priority}. Must be 1, 2, or 3.")
        
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        
        try:

            # Create new job
            job_id = self._generate_job_id()
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
                "priority_updated_timestamp": None,
                "metadata": {}
            }
            if job_metadata:
                job_data['metadata'] = job_metadata
            result = self.redis_conn.evalsha(
                self.enqueue_atomic_script,
                4,
                self.JOB_QUEUE_KEY,
                self.JOB_HASH_KEY,
                self.ENTITY_INDEX_KEY,
                self.PROCESSING_ENTITIES_KEY,
                entity_id,
                job_id,
                json.dumps(job_data),
                priority,
                now
            )
            
            result_str = result.decode() if isinstance(result, bytes) else result
            status, returned_id = result_str.split(':', 1)

            if status == "UPDATED":
                self.logger.info(f"Updating priority for entity {entity_id} to {priority}")
            elif status == "EXISTS":
                pass
            elif status == "PROCESSING":
                self.logger.info(f"Entity {entity_id} is currently being processed by job {returned_id}")
            elif status == "CREATED":
                self.logger.info(f"Enqueued job {returned_id} for entity {entity_id} with priority {priority}")
                
            return returned_id
            
        except RedisError as e:
            self.logger.error(f"Failed to enqueue job: {e}")
            raise RedisError(f"Failed to enqueue job: {e}")
    
    def update_priority(self, identifier: str, new_priority: int) -> str:
        """
        Update the priority of an existing job.
        
        Args:
            identifier: Either a job_id or entity_id
            new_priority: New priority level (1=highest, 2=medium, 3=lowest)
            
        Returns:
            str: The job_id that was updated
            
        Raises:
            ValueError: If job not found or invalid priority
            RedisError: If Redis operations fail
        """
        if new_priority not in [1, 2, 3]:
            raise ValueError(f"Invalid priority: {new_priority}. Must be 1, 2, or 3.")
        
        try:
            # Try as job_id first
            raw_job = self.redis_conn.hget(self.JOB_HASH_KEY, identifier)
            if raw_job:
                job_id = identifier
            else:
                # Try as entity_id
                job_id_lookup = self.redis_conn.hget(self.ENTITY_INDEX_KEY, identifier)
                if not job_id_lookup:
                    self.logger.warning(f"Job not found for identifier: {identifier}")
                    raise ValueError(f"Job not found for identifier: {identifier}")
                job_id = job_id_lookup
            
            return self._apply_priority_update(job_id, new_priority)
            
        except RedisError as e:
            self.logger.error(f"Failed to update priority: {e}")
            raise RedisError(f"Failed to update priority: {e}")
    
    def _apply_priority_update(self, job_id: str, new_priority: int) -> str:
        """Internal method to apply priority update to a job."""
        job_id_bytes = job_id if isinstance(job_id, bytes) else job_id.encode()
        
        job_json = self.redis_conn.hget(self.JOB_HASH_KEY, job_id_bytes)
        if not job_json:
            self.logger.error(f"Job data not found for job_id: {job_id}")
            raise ValueError(f"Job data not found for job_id: {job_id}")
        
        job_data = json.loads(job_json)
        job_data['priority'] = new_priority
        job_data['priority_updated_timestamp'] = datetime.now(timezone.utc).isoformat()
        
        self.redis_conn.hset(self.JOB_HASH_KEY, job_id_bytes, json.dumps(job_data))
        self.redis_conn.zadd(self.JOB_QUEUE_KEY, {job_id_bytes: new_priority})
        
        return job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
    
    def get_status(self, identifier: str) -> Dict[str, Any]:
        """
        Get the status of a job by job_id or entity_id.
        
        Args:
            identifier: Either a job_id or entity_id
            
        Returns:
            Dict containing job_id, position_in_queue, and priority
            
        Raises:
            ValueError: If job not found
            RedisError: If Redis operations fail
        """
        try:
            job_id_bytes = identifier.encode() if isinstance(identifier, str) else identifier
            job_json = self.redis_conn.hget(self.JOB_HASH_KEY, job_id_bytes)
            if not job_json:
                job_id_lookup = self.redis_conn.hget(self.ENTITY_INDEX_KEY, identifier)
                if not job_id_lookup:
                    raise ValueError(f"Job not found for identifier: {identifier}")
                job_id_bytes = job_id_lookup
                job_json = self.redis_conn.hget(self.JOB_HASH_KEY, job_id_bytes)
            
            job_data = json.loads(job_json)
            entity_id = job_data.get('entity_id')
            job_id_str = job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
            overall_position = self.redis_conn.zrank(self.JOB_QUEUE_KEY, job_id_bytes)

            response = {
                "uuid": job_data.get('metadata', {}).get('uuid'),
                "hubmap_id": job_data.get('metadata', {}).get('hubmap_id'),
                "priority": job_data.get('priority'),
                "queued_timestamp": job_data.get('queued_timestamp'),
                "priority_updated_timestamp": job_data.get('priority_updated_timestamp'),
                "status": "queued"
            }

            if overall_position is not None:
                priority_score = job_data.get('priority')
                items_ahead = self.redis_conn.zrange(self.JOB_QUEUE_KEY, 0, overall_position - 1, withscores=True)
                count_same_priority = sum(1 for _, score in items_ahead if int(score) == priority_score)

                response["status"] = "queued"
                response["execution_number_in_priority"] = count_same_priority + 1
            
            elif self.redis_conn.hexists(self.PROCESSING_ENTITIES_KEY, entity_id):
                response["status"] = "processing"
                response["process_start_timestamp"] = job_data.get('process_start_timestamp')
            else:
                raise ValueError(f"Job {job_id_str} is no longer in the active system.")

            return response
            
        except RedisError as e:
            self.logger.error(f"Failed to get job status: {e}")
            raise RedisError(f"Failed to get job status: {e}")
        except ValueError as e:
            self.logger.error(f"Failed to get job status: {e}")
            raise ValueError(f"Failed to get job status: {e}")
        except Exception as e:
            self.logger.error(f"Failed to get job status: {e}")
            raise Exception(f"Failed to get job status: {e}")
    
    def get_queue_status(self, all_queued: bool = False, all_processing: bool = False) -> Dict[str, Any]:
        """
        Get overall queue status.
        
        Returns:
            Dict containing total jobs and count by priority
            
        Raises:
            RedisError: If Redis operations fail
        """
        try:
            active_jobs_count = self.redis_conn.hlen(self.PROCESSING_ENTITIES_KEY)
            p1 = self.redis_conn.zcount(self.JOB_QUEUE_KEY, 1, 1)
            p2 = self.redis_conn.zcount(self.JOB_QUEUE_KEY, 2, 2)
            p3 = self.redis_conn.zcount(self.JOB_QUEUE_KEY, 3, 3)
            
            self.logger.debug(f"Queue status: {active_jobs_count} processing, {p1 + p2 + p3} queued")
            status = {
                "jobs_processing": active_jobs_count,
                "priority_1_queued": p1,
                "priority_2_queued": p2,
                "priority_3_queued": p3
            }

            total_processed_raw = self.redis_conn.get(self.TOTAL_PROCESSED_KEY)
            min_time = self.redis_conn.get(self.MIN_RUNTIME_KEY)
            max_time = self.redis_conn.get(self.MAX_RUNTIME_KEY)
            uptime_seconds = time.perf_counter() - self.service_start_time
            durations = self.redis_conn.lrange('job_durations', 0, -1)
            status["total_jobs_processed"] = int(total_processed_raw) if total_processed_raw else 0
            status['min_job_runtime'] = round(float(min_time) / 1000, 2) if min_time else 0
            status['max_job_runtime'] = round(float(max_time) / 1000, 2) if max_time else 0
            status["uptime"] = int(uptime_seconds)
            if durations:
                floats = [float(d) for d in durations]
                avg_ms = sum(d for d in floats) / len(durations)
                avg_seconds = (avg_ms/1000)
                rounded_avg_seconds = round(avg_seconds, 2)
                status["avg_job_runtime"] = rounded_avg_seconds
            else:
                status["avg_job_runtime"] = 0
            if all_queued:
                queued_ids = self.redis_conn.zrange(self.JOB_QUEUE_KEY, 0, -1)
                status["queued_entities"] = self._get_detailed_info(queued_ids, mode='all_queued')
            if all_processing:
                processing_job_ids = self.redis_conn.hvals(self.PROCESSING_ENTITIES_KEY) 
                status["processing_entities"] = self._get_detailed_info(processing_job_ids, mode='all_processing')

            return status
            
        except RedisError as e:
            self.logger.error(f"Failed to get queue status: {e}")
            raise RedisError(f"Failed to get queue status: {e}")
    
    def _get_detailed_info(self, job_ids: list, mode: str) -> list:
        """Helper to fetch and format metadata for a list of job IDs."""
        if not job_ids:
            return []
        
        details = []
        job_metadatas = self.redis_conn.hmget(self.JOB_HASH_KEY, job_ids)

        for job_json in job_metadatas:
            if job_json:
                data = json.loads(job_json)
                metadata = data.get("metadata", {})
                details_dict = {
                    "metadata": metadata,
                    "priority": data.get("priority"),
                    "queued_timestamp": data.get("queued_timestamp"),
                    "priority_updated_timestamp": data.get("priority_updated_timestamp")
                }
                if mode == 'all_processing':
                    details_dict["process_start_timestamp"] = data.get("process_start_timestamp")
                details.append(details_dict)
        return details
    
    def _execute_job(self, job_data: Dict[str, Any]) -> bool:
        """
        Execute a job by dynamically importing and calling the task function.
        
        Args:
            job_data: Job metadata including module, function name, and arguments
            
        Returns:
            bool: True if job executed successfully, False otherwise
        """
        module_name = job_data['task_module']
        func_name = job_data['task_name']
        args = job_data.get('args', [])
        kwargs = job_data.get('kwargs', {})
        job_id = job_data.get('job_id', 'unknown')
        
        try:
            self.logger.debug(f"Executing job {job_id}: {module_name}.{func_name}")
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            func(*args, **kwargs)
            self.logger.info(f"Job {job_id} executed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error executing job {job_id}: {e}", exc_info=True)
            return False
    
    def _worker_loop(self):
        """
        Main worker loop. Continuously polls Redis for jobs and executes them.
        This runs in a subprocess spawned by start_workers().
        """
        self.logger.info(f"Worker (PID {subprocess.os.getpid()}) starting up")
        
        while True:
            try:
                # Pop job from queue atomically
                now = datetime.now(timezone.utc).isoformat()
                result = self.redis_conn.evalsha(
                    self.pop_and_fetch_script, 
                    3, 
                    self.JOB_QUEUE_KEY, 
                    self.JOB_HASH_KEY,
                    self.PROCESSING_ENTITIES_KEY,
                    now
                )
            except RedisError as e:
                self.logger.critical(f"FATAL: Error during job pop: {e}. Worker exiting.")
                break
            
            if not result:
                # No jobs available, sleep briefly
                time.sleep(0.5)
                continue
            
            # Parse job data
            job_id = result[0]
            job_id_str = job_id.decode() if isinstance(job_id, bytes) else job_id
            job_json = result[1]
            job_data = json.loads(job_json)
        
            entity_id = job_data['entity_id']
            self.logger.info(f"Worker (PID {subprocess.os.getpid()}) starting job: {job_id_str} (Priority {job_data.get('priority', '?')})")

            start_time = time.perf_counter()
            try:
                job_successful = self._execute_job(job_data)
            finally:
                # Clean up job metadata
                try:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    self.redis_conn.evalsha(
                        self.set_min_max_script,
                        2,
                        self.MIN_RUNTIME_KEY,
                        self.MAX_RUNTIME_KEY,
                        duration_ms
                    )
                    # min_time = self.redis_conn.get(self.MIN_RUNTIME_KEY)
                    # max_time = self.redis_conn.get(self.MAX_RUNTIME_KEY)
                    # if min_time:
                    #     if duration_ms < min_time:
                    #         self.redis_conn.set(self.MIN_RUNTIME_KEY, duration_ms)
                    # else:
                    #     self.redis_conn.set(self.MIN_RUNTIME_KEY, duration_ms)
                    # if max_time:
                    #     if duration_ms > max_time:
                    #         self.redis_conn.set(self.MAX_RUNTIME_KEY, duration_ms)
                    # else:
                    #     self.redis_conn.set(self.MAX_RUNTIME_KEY, duration_ms)
                    self.redis_conn.lpush('job_durations', duration_ms)
                    self.redis_conn.ltrim('job_durations', 0, 49)
                    self.redis_conn.incr(self.TOTAL_PROCESSED_KEY)
                    self.redis_conn.hdel(self.JOB_HASH_KEY, job_id)
                    self.redis_conn.hdel(self.ENTITY_INDEX_KEY, entity_id)
                    self.redis_conn.hdel(self.PROCESSING_ENTITIES_KEY, entity_id)
                    
                    if job_successful:
                        self.logger.info(f"Worker (PID {subprocess.os.getpid()}) completed job {job_id_str} with uuid {entity_id} successfully. Metadata cleaned.")
                    else:
                        self.logger.warning(f"Worker (PID {subprocess.os.getpid()}) job {job_id_str} failed. Metadata cleaned.")
                        
                except RedisError as e:
                    self.logger.warning(f"FATAL Cleanup Error for job {job_id_str}: {e}. Worker forced to exit.")
                    break
        
        self.logger.info(f"Worker (PID {subprocess.os.getpid()}) shut down")
    
    def start_workers(self, num_workers: int = 4):
        """
        Spawn worker subprocesses that continuously process jobs from the queue.
        
        This method spawns multiple subprocess workers and blocks until interrupted.
        Each worker runs independently and processes jobs as they become available.
        
        Args:
            num_workers: Number of worker processes to spawn (default: 4)
            
        Note:
            This method blocks and should be called from a dedicated worker script,
            not from the Flask application. Use Ctrl+C to terminate all workers.
        """
        import sys
        
        processes = []
        
        try:
            self.logger.info(f"Starting {num_workers} worker processes...")
            
            for i in range(num_workers):
                # Fork a new process that runs the worker loop
                proc = subprocess.Popen([
                    sys.executable, "-c",
                    "import sys; "
                    #Explicitly add the application source roots to sys.path for worker processes.
                    #These subprocesses are launched outside the normal entrypoint and do not
                    #inherit the same working directory or import context as the main service.
                    #Adding these paths ensures imports resolve against the project source tree
                    #rather than relying on implicit CWD behavior, which may change if the
                    #repository layout is modified in the future.
                    "sys.path.insert(0, '/usr/src/app/src'); "
                    "sys.path.insert(0, '/usr/src/app/src/search-adaptor/src'); "
                    f"from {self.__module__} import JobQueue; "
                    f"queue = JobQueue('{self.redis_conn.connection_pool.connection_kwargs['host']}', "
                    f"{self.redis_conn.connection_pool.connection_kwargs['port']}, "
                    f"{self.redis_conn.connection_pool.connection_kwargs['db']}); "
                    "queue._worker_loop()"
                ])
                processes.append(proc)
                self.logger.info(f"Started worker {i+1}/{num_workers} (PID {proc.pid})")
                time.sleep(0.05)  # Small delay to avoid flooding
            
            self.logger.info(f"All {num_workers} workers started. Press Ctrl+C to terminate.")

            # Wait for all workers
            for proc in processes:
                proc.wait()
                
        except KeyboardInterrupt:
            self.logger.info("\nTerminating all workers...")
            for proc in processes:
                proc.terminate()
            for proc in processes:
                proc.wait()
            self.logger.info("All workers terminated.")
        except Exception as e:
            self.logger.info(f"Error managing workers: {e}")
            for proc in processes:
                if proc.poll() is None:
                    proc.terminate()
            raise