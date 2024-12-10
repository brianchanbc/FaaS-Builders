import argparse
import redis
import zmq
import threading
from typing import Dict
from collections import deque

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from utils.task_handler import execute_fn
from utils.redis_functions import get_hash_table_field, get_hash_table, set_hash_table_field
from utils.serializer import serialize, deserialize
from collections import deque
from queue import Queue
import multiprocessing
from time import sleep, time
from utils.models import Task
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT = 5
HEARTBEAT_TIMEOUT = 5

# for local worker, process the results from the result queue
def result_processor(result_queue):
    # Initialize Redis client and logger in the process
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    logger = logging.getLogger(__name__)
    logger.info("Result processor started")

    while True:
        try:
            task_id, status, result = result_queue.get()
            if task_id is None:  # Exit signal
                logger.info("Result processor exiting")
                break

            task_key = f"task:{task_id}"
            set_hash_table_field(redis_client, task_key, "status", status)
            if status == "COMPLETE":
                set_hash_table_field(redis_client, task_key, "result", serialize(result))
        except Exception as e:
            logger.error(f"Error processing result: {str(e)}")
            raise e


# for local worker, execute the task
def execute_task(task_id, fn_payload, param_payload):
    fn = deserialize(fn_payload)
    args, kwargs = deserialize(param_payload)

    try:
        result = fn(*args, **kwargs)
        return task_id, "COMPLETE", result
    except Exception as e:
        return task_id, "FAILED", str(e)


# local worker takes a task from the task queue, executes it and puts the result in the result queue
def local_worker(worker_id, task_queue, result_queue):
    logger = logging.getLogger(__name__)
    logger.info(f"Worker {worker_id} started")

    while True:
        try:
            task = task_queue.get()
            if task == (None, None, None):  # Exit signal
                logger.info(f"Worker {worker_id} exiting")
                break

            task_id, fn_payload, param_payload = task
            try:
                logger.info(f"Worker {worker_id} processing task {task_id}")
                result = execute_fn(task_id, fn_payload, param_payload)
                result_queue.put((task_id, "COMPLETE", result))
            except Exception as e:
                logger.error(f"Worker {worker_id} failed task {task_id}: {str(e)}")
                result_queue.put((task_id, "FAILED", f"Error: {str(e)}"))
        except Exception as e:
            logger.error(f"Error retrieving task in worker {worker_id}: {str(e)}")
            break



def monitor_tasks(task_queue: Queue, shutdown_event: threading.Event):
    running_jobs = deque()
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    while not shutdown_event.is_set():
        try:
            # Check for new tasks from main process
            while not task_queue.empty():
                task = task_queue.get_nowait()
                running_jobs.append(task)
                
            while running_jobs and time() > running_jobs[0].task_time_out:
                task = running_jobs.popleft()
                task_status = get_hash_table_field(redis_client, task.task_id, 'status')
                if task_status in ['RUNNING']:
                    set_hash_table_field(redis_client, task.task_id, 'status', 'FAILED')
                    set_hash_table_field(redis_client, task.task_id, 'result', 
                                       serialize('WorkerFailure: Task execution timed out'))
                    logger.info(f"Task {task.task_id} timed out")
        except Exception as e:
            logger.error(f"Error in task monitor: {e}")
    logger.info("Monitor thread shutting down")

def start_dispatcher(mode, port, num_workers):
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    context = zmq.Context()
    
    if mode == 'local':
        logger.info("Start dispatcher in local mode")

        # Set up the task queue and result queue for communication between processes
        task_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()

        # Start workers using multiprocessing
        workers = []
        for i in range(num_workers):
            worker_process = multiprocessing.Process(target=local_worker, args=(i, task_queue, result_queue))
            worker_process.start()
            workers.append(worker_process)

        # Start the result processor
        result_processor_process = multiprocessing.Process(target=result_processor, args=(result_queue,))
        result_processor_process.start()

        # Set up the Redis pub/sub to listen for tasks
        pubsub = redis_client.pubsub()
        pubsub.subscribe("Tasks")

        try:
            for message in pubsub.listen():
                if message["type"] == "message":
                    task_id = deserialize(message["data"]).get("task_id")
                    task_key = f"task:{task_id}"
                    
                    # Set task status to RUNNING in Redis
                    set_hash_table_field(redis_client, task_key, 'status', 'RUNNING')

                    # Retrieve function and parameters
                    function_id = get_hash_table_field(redis_client, task_key, 'function_id')
                    fn_payload = get_hash_table_field(redis_client, f"function:{function_id}", 'payload')
                    param_payload = get_hash_table_field(redis_client, task_key, 'param_payload')

                    # Add the task to the task queue
                    task_queue.put((task_id, fn_payload, param_payload))
        except Exception as e:
            logger.error(f"Task dispatcher error: {str(e)}")
        finally:
            # Send termination signals to all workers
            for _ in range(num_workers):
                task_queue.put((None, None, None))
            for worker in workers:
                worker.join()

        # Send termination signal to the result processor and wait for it to finish
        result_queue.put((None, None, None))
        result_processor_process.join()

        logger.info("Task dispatcher exiting")


    elif mode == 'push':
        logger.info("Start dispatcher in push mode")
        socket = context.socket(zmq.ROUTER)
        socket.bind(f"tcp://127.0.0.1:{port}")

        pending_tasks = {}
        worker_heartbeat = {}
        task_status = {}  # task_id -> {"worker_id", "fn_payload", "param_payload", "start_time"}
        worker_task_counts = {}
        function_cache = {}

        def register_worker(worker_id):
            worker_heartbeat[worker_id] = time()
            if worker_id not in worker_task_counts:
                worker_task_counts[worker_id] = 0
                logger.info(f"Worker {worker_id} registered")

        def check_worker_heartbeat():
            current_time = time()
            for worker_id, last_heartbeat in list(worker_heartbeat.items()):
                if current_time - last_heartbeat > HEARTBEAT_TIMEOUT:
                    logger.warning(f"Worker {worker_id} heartbeat timeout")
                    handle_worker_failure(worker_id)

        def handle_worker_failure(worker_id):
            worker_heartbeat.pop(worker_id, None)
            worker_task_counts.pop(worker_id, None)

            for task_id, task_info in list(task_status.items()):
                if task_info["worker_id"] == worker_id:
                    if task_info.get("start_time"):
                        # Task started, mark as failed
                        logger.warning(f"Task {task_id} failed due to worker {worker_id} failure")
                        task_key = f"task:{task_id}"
                        set_hash_table_field(redis_client, task_key, "status", "FAILED")
                        set_hash_table_field(redis_client, task_key, "result", "WorkerFailure: Worker failed during task execution")
                    else:
                        # Task assigned but not started, requeue it
                        logger.warning(f"Requeuing task {task_id} due to worker {worker_id} failure")
                        pending_tasks[task_id] = (task_info["fn_payload"], task_info["param_payload"])

                    task_status.pop(task_id)

        def dispatch_task():
            while pending_tasks and worker_task_counts:
                worker_id = min(worker_task_counts, key=worker_task_counts.get)
                task_id, (fn_payload, param_payload) = pending_tasks.popitem()

                try:
                    task_data = serialize({
                        "task_id": task_id,
                        "fn_payload": fn_payload,
                        "param_payload": param_payload
                    })
                    socket.send_multipart([f"task_receiver_{worker_id}".encode(), task_data.encode("utf-8")])
                    logger.info(f"Task {task_id} dispatched to worker {worker_id}")

                    # Update task state
                    task_status[task_id] = {
                        "worker_id": worker_id,
                        "fn_payload": fn_payload,
                        "param_payload": param_payload,
                        "start_time": None
                    }
                    worker_task_counts[worker_id] += 1
                except Exception as e:
                    logger.error(f"Failed to dispatch task {task_id}: {e}")
                    pending_tasks[task_id] = (fn_payload, param_payload)

        def handle_ack(worker_id, task_id):
            if task_id in task_status:
                task_status[task_id]["start_time"] = time()
                logger.info(f"Task {task_id} acknowledged by worker {worker_id}")

        def handle_result(worker_id, result_data):
            task_id = result_data["task_id"]
            status = result_data["status"]
            result = result_data["result"]
            task_key = f"task:{task_id}"

            logger.info(f"Worker {worker_id} completed task {task_id} with status {status}")
            set_hash_table_field(redis_client, task_key, "status", status)
            set_hash_table_field(redis_client, task_key, "result", result)

            if task_id in task_status:
                task_status.pop(task_id)
            if worker_id in worker_task_counts:
                worker_task_counts[worker_id] -= 1

        def check_task_execution_time():
            current_time = time()
            for task_id, task_info in list(task_status.items()):
                if task_info.get("start_time") and current_time - task_info["start_time"] > DEFAULT_TIMEOUT:
                    logger.warning(f"Task {task_id} exceeded max execution time")
                    task_key = f"task:{task_id}"
                    set_hash_table_field(redis_client, task_key, "status", "FAILED")
                    set_hash_table_field(redis_client, task_key, "result", "Timeout: Task execution exceeded time limit")
                    task_status.pop(task_id)
                    worker_id = task_info["worker_id"]
                    if worker_id in worker_task_counts:
                        worker_task_counts[worker_id] -= 1

        def listen_to_redis():
            pubsub = redis_client.pubsub()
            pubsub.subscribe("Tasks")
            try:
                for message in pubsub.listen():
                    if message["type"] == "message":
                        task_data = deserialize(message["data"])
                        task_id = task_data["task_id"]
                        task_key = f"task:{task_id}"

                        function_id = get_hash_table_field(redis_client, task_key, "function_id")
                        if function_id not in function_cache:
                            function_cache[function_id] = get_hash_table_field(redis_client, f"function:{function_id}", "payload")

                        fn_payload = function_cache[function_id]
                        param_payload = get_hash_table_field(redis_client, task_key, "param_payload")
                        pending_tasks[task_id] = (fn_payload, param_payload)
                        logger.info(f"Task {task_id} added to pending queue")
            except Exception as e:
                logger.error(f"Redis listener encountered an error: {e}")

        threading.Thread(target=listen_to_redis, daemon=True).start()

        while True:
            try:
                message = socket.recv_multipart(flags=zmq.NOBLOCK)
                worker_id = message[0].decode()
                data = deserialize(message[1].decode("utf-8"))

                if data["TYPE"] == "HEARTBEAT":
                    register_worker(worker_id[10:])
                elif data["TYPE"] == "ACK":
                    handle_ack(worker_id[15:], data["task_id"])
                elif data["TYPE"] == "RESULT":
                    handle_result(worker_id[14:], data["DATA"])
            except zmq.Again:
                pass

            check_worker_heartbeat()
            check_task_execution_time()
            dispatch_task()
            sleep(0.1)


    elif mode == 'pull':
        logger.info("Start dispatcher in pull mode")
        rep_socket = context.socket(zmq.REP)
        rep_socket.bind(f"tcp://127.0.0.1:{port}")
        workers = {}
        active_tasks = {}
        
        # Start monitor process with shutdown event
        task_queue = Queue()
        shutdown_event = threading.Event()
        monitor_process = threading.Thread(
            target=monitor_tasks,
            args=(task_queue, shutdown_event)
        )
        monitor_process.daemon = True
        monitor_process.start()
        
        try:
            while True:
                # Receive message from worker
                logger.info("Receive message from worker")
                message_str = rep_socket.recv().decode('utf-8')
                request = deserialize(message_str)
                worker_id = request["worker_id"]
                
                # Register worker
                if request["message_type"] == "HANDSHAKE":
                    logger.info(f"Worker {worker_id} handshake")
                    workers[worker_id] = worker_id
                    rep_socket.send_string("Handshake ACK")  
                    continue
                
                # Check if worker is registered
                if worker_id not in workers:
                    logger.error(f"Worker {worker_id} not registered")
                    rep_socket.send_string("ERROR: Worker not registered yet")  
                    continue
                
                # Handle worker task request
                if request["message_type"] == "TASK_REQUEST":
                    logger.info(f"Worker {worker_id} request task")                    
                    logger.info(f"Worker {worker_id} checking for new tasks")
                    tasks = redis_client.keys('task:*')
                    task_found = False
                    for task_key in tasks:
                        if task_key in active_tasks:
                            continue
                        active_tasks[task_key] = task_key
                        task_status = get_hash_table_field(redis_client, task_key, 'status')
                        if task_status == "QUEUED":
                            # Get task
                            task = get_hash_table(redis_client, task_key)
                            logger.info(f"Task {task_key} found")
                            function_id = task.get('function_id')
                            fn_key = f"function:{function_id}"
                            fn_payload = get_hash_table_field(redis_client, fn_key, 'payload')
                            param_payload = get_hash_table_field(redis_client, task_key, 'param_payload')
                            assign_task = Task(
                                task_id=task_key,
                                task_start_time=time(),
                                task_time_out=time() + DEFAULT_TIMEOUT,
                                fn=fn_payload,
                                params=param_payload
                            )
                            # Reply back to worker who requested the task
                            set_hash_table_field(redis_client, task_key, 'status', 'RUNNING')
                            logger.info(f"Task {task_key} status set to RUNNING")
                            # Send to monitor process
                            task_queue.put(assign_task) 
                            rep_socket.send_string(serialize(assign_task))
                            logger.info(f"Worker {worker_id} assigned new task")
                            task_found = True
                            break
                    if not task_found:
                        rep_socket.send_string(serialize("NO TASK"))
                        logger.info(f"Worker {worker_id} no task found")
                # Handle worker task result
                elif request["message_type"] == "TASK_RESULT":
                    logger.info(f"Worker {worker_id} completed task and return result")
                    task_status = request["status"]
                    result = request["result"]
                    task_id = request["task_id"]
                    worker_id = request["worker_id"]
                    task_payload = request["task"]
                    if task_status == "COMPLETE":
                        # Handle task completed successfully
                        set_hash_table_field(redis_client, task_id, 'status', 'COMPLETE')
                        logger.info(f"Task {task_id} status set to COMPLETE")
                    elif task_status == "FAILED":
                        # Handle task failed
                        set_hash_table_field(redis_client, task_id, 'status', 'FAILED')
                        logger.info(f"Task {task_id} status set to FAILED")
                    set_hash_table_field(redis_client, task_id, 'result', result)
                    logger.info(f"Result for task {task_id} is {deserialize(result)}")
                    active_tasks.pop(task_id, None)
                    rep_socket.send_string("ACK")
                # Other message types are invalid
                else:
                    rep_socket.send_string("ERROR")  
                    logger.error("Invalid message type")
                    raise ValueError("Invalid message type")                
        except Exception as e:
            rep_socket.send_string("ERROR") 
            logger.error(f"Error: {str(e)}")
            shutdown_event.set() 
            monitor_process.join(timeout=2.0)
            context.destroy()
            raise RuntimeError(f"Failed to pull tasks from Redis: {str(e)}")
        finally:
            shutdown_event.set()  
            monitor_process.join(timeout=2.0)
            context.destroy()
            logger.info("Task dispatcher exiting")
            
    else:
        logger.error("Invalid mode")
        raise ValueError("Invalid mode")

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('-m', type=str, required=True, choices=['local', 'push', 'pull'], 
                     help='Mode (local, push, pull)')
    args.add_argument('-p', type=int, default=-1, help='Port')
    args.add_argument('-w', type=int, default=-1, help='Number of workers')
    
    parsed = args.parse_args()
    start_dispatcher(parsed.m, parsed.p, parsed.w)