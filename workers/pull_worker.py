import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import zmq
import argparse
import uuid
from multiprocessing import Pool, Lock
from utils.serializer import serialize, deserialize
from utils.task_handler import execute_fn
import logging
import time
from queue import Queue
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def start_worker(num_processors, dispatcher_url):
    context = zmq.Context()
    req_socket = context.socket(zmq.REQ)
    req_socket.connect(dispatcher_url)
    worker_id = str(uuid.uuid4())

    socket_lock = Lock()
    task_queue = Queue()
    
    # Handshake with dispatcher
    handshake = {
        "message_type": "HANDSHAKE",
        "worker_id": worker_id,
    }
    while True:
        try:
            with socket_lock:  
                logger.info(f"Worker {worker_id} sending handshake")
                req_socket.send_string(serialize(handshake))
                message = req_socket.recv().decode('utf-8')
                logger.info(f"Worker {worker_id} received {message}")
            if message == "Handshake ACK":
                break
        except Exception as e:
            logger.error(e)
            raise e("Failed to handshake with dispatcher")  
    
    # Request tasks from dispatcher
    with Pool(num_processors) as pool:
        pending_tasks = {}  
        
        while True:
            # Request new task if we have capacity
            if len(pending_tasks) < num_processors:
                task_request = {
                    "message_type": "TASK_REQUEST",
                    "worker_id": worker_id,
                }
                try:
                    with socket_lock:
                        logger.info(f"Worker {worker_id} requesting task")
                        req_socket.send_string(serialize(task_request))
                        response = req_socket.recv().decode('utf-8')
                        task_payload = deserialize(response)
                    
                        if not isinstance(task_payload, str):
                            task_queue.put(task_payload)
                except Exception as e:
                    logger.error(f"Error requesting new task: {e}")
                time.sleep(0.5)

            # Get new task from queue if we have capacity
            if len(pending_tasks) < num_processors and not task_queue.empty():
                task_payload = task_queue.get()
                task_id = task_payload.task_id
                fn = task_payload.fn
                params = task_payload.params
                time_out = task_payload.task_time_out
                
                logger.info(f"Assigning task {task_id} to worker {worker_id} to execute")
                async_result = pool.apply_async(execute_fn, (task_id, fn, params,))
                pending_tasks[task_id] = (async_result, task_payload, time_out)

            # Check all pending tasks
            completed_tasks = []
            for task_id, (async_result, task_payload, time_out) in pending_tasks.items():
                try:
                    if not async_result.ready():
                        # If task is not ready, skip
                        continue

                    completed_tasks.append(task_id)
                    result = async_result.get(timeout=0)  
                    
                    if time.time() > time_out:
                        logger.error(f"Worker {worker_id} task {task_id} timed out")
                        continue
                    
                    with socket_lock:
                        task_result = {
                            "message_type": "TASK_RESULT",
                            "task_id": task_id,
                            "result": result,
                            "status": "COMPLETE",
                            "worker_id": worker_id,
                            "task": task_payload,
                        }
                        req_socket.send_string(serialize(task_result))
                        ack = req_socket.recv().decode('utf-8')
                        logger.info(f"Worker {worker_id} completed task {task_id} and received ack")

                except Exception as e:
                    completed_tasks.append(task_id)
                    logger.error(f"Exception type: {type(e).__name__}")
                    if type(e).__name__ == "TimeoutError":
                        logger.error(f"Worker {worker_id} task {task_id} timed out")
                        continue

                    logger.error(f"Worker {worker_id} failed in task execution: {e}")
                    with socket_lock:
                        task_result = {
                            "message_type": "TASK_RESULT",
                            "task_id": task_id,
                            "result": serialize(f"FailedFunctionError: Failed task execution due to {e}"),
                            "status": "FAILED",
                            "worker_id": worker_id,
                            "task": task_payload,
                        }
                        req_socket.send_string(serialize(task_result))
                        ack = req_socket.recv().decode('utf-8')

            # Remove completed tasks
            for task_id in completed_tasks:
                pending_tasks.pop(task_id, None)

            time.sleep(0.5) 
    

if __name__ == '__main__':
    context = zmq.Context()
    parser = argparse.ArgumentParser()
    parser.add_argument('num_processors', type=int, help='Number of worker processors')
    parser.add_argument('dispatcher_url', type=str, help='Dispatcher URL')
    args = parser.parse_args()
    start_worker(args.num_processors, args.dispatcher_url)