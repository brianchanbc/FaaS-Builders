import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import zmq
import time
import uuid
from multiprocessing import Process, Queue
from threading import Thread
import logging
from utils.serializer import serialize, deserialize
from utils.task_handler import execute_fn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

context = zmq.Context()

def heartbeat_process(worker_id, dispatcher_url):
    logger.info(f"Starting heartbeat process for worker: {worker_id}")
    global context

    socket = context.socket(zmq.DEALER)
    socket.identity = f"heartbeat_{worker_id}".encode()
    socket.connect(dispatcher_url)

    while True:
        socket.send_multipart([serialize({"TYPE": "HEARTBEAT"}).encode("utf-8")])
        time.sleep(1)


def task_receiver_process(worker_id, dispatcher_url, task_queue):
    logger.info(f"Starting task receiver process for worker: {worker_id}")
    global context

    socket = context.socket(zmq.DEALER)
    socket.identity = f"task_receiver_{worker_id}".encode()
    socket.connect(dispatcher_url)

    while True:
        message = socket.recv_multipart()
        task_data = deserialize(message[0].decode("utf-8"))
        logger.info(f"Received task: {task_data["task_id"]}")
        task_queue.put(task_data)


def task_processor_process(worker_id, dispatcher_url, task_queue, result_queue):
    logger.info(f"Starting task processor process for worker: {worker_id}")
    global context

    socket = context.socket(zmq.DEALER)
    socket.identity = f"task_processor_{worker_id}".encode()
    socket.connect(dispatcher_url)

    while True:
        task_data = task_queue.get()
        if task_data is None:  # Exit signal
            break

        task_id = task_data["task_id"]
        fn_payload = task_data["fn_payload"]
        param_payload = task_data["param_payload"]

        # Send acknowledgment to the dispatcher
        socket.send_multipart([serialize({"TYPE": "ACK", "task_id": task_id}).encode("utf-8")])

        # Execute the task
        try:
            result = execute_fn(task_id, fn_payload, param_payload)
            result_queue.put({"task_id": task_id, "result": result, "status": "COMPLETE"})
        except Exception as e:
            result_queue.put({"task_id": task_id, "result": serialize(f"FailedFunctionError: Failed task execution due to {str(e)}"), "status": "FAILED"})


def result_sender_process(worker_id, dispatcher_url, result_queue):
    logger.info(f"Starting result sender process for worker: {worker_id}")
    global context

    socket = context.socket(zmq.DEALER)
    socket.identity = f"result_sender_{worker_id}".encode()
    socket.connect(dispatcher_url)

    while True:
        result_data = result_queue.get()
        if result_data is None:  # Exit signal
            break
        socket.send_multipart([serialize({"TYPE": "RESULT", "DATA": result_data}).encode("utf-8")])


class PushWorker:
    def __init__(self, worker_id, dispatcher_url, num_processors):
        self.worker_id = worker_id
        self.dispatcher_url = dispatcher_url
        self.num_processors = num_processors
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.processes = []

    def run(self):
        # Start the heartbeat process
        Thread(target=heartbeat_process, args=(self.worker_id, self.dispatcher_url), daemon=True).start()

        # Start the task receiver process
        Thread(target=task_receiver_process, args=(self.worker_id, self.dispatcher_url, self.task_queue), daemon=True).start()

        # Start task processor processes
        for _ in range(self.num_processors):
            p = Thread(target=task_processor_process, args=(self.worker_id, self.dispatcher_url, self.task_queue, self.result_queue), daemon=True)
            p.start()
            self.processes.append(p)

        # Start the result sender process
        result_sender_process_obj = Thread(target=result_sender_process, args=(self.worker_id, self.dispatcher_url, self.result_queue), daemon=True)
        result_sender_process_obj.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down worker")
        finally:
            # Send exit signals to child processes
            for _ in range(self.num_processors):
                self.task_queue.put(None)
            self.result_queue.put(None)

            # Wait for task processor processes to terminate
            for p in self.processes:
                p.join()

            # Wait for the result sender process to terminate
            result_sender_process_obj.join()



if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python push_worker.py <num_worker_processors> <dispatcher_url>")
        sys.exit(1)

    num_processors = int(sys.argv[1])
    dispatcher_url = sys.argv[2]

    worker_id = str(uuid.uuid4())
    worker = PushWorker(worker_id, dispatcher_url, num_processors)
    worker.run()