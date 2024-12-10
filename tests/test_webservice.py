import requests
from utils.serializer import serialize, deserialize
from utils.redis_functions import delete_everything
import logging
import time
import random
import threading
from queue import Queue
import redis
import pytest

base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETE", "FAILED"]


@pytest.fixture(autouse=True)
def clean_redis():
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    delete_everything(redis_client)
    yield

def double(x):
    #time.sleep(1)
    return x * 2

def wait_fn(seconds):
    time.sleep(seconds)
    x = double(seconds)
    return x

def wait_failed_fn(seconds):
    time.sleep(seconds)
    # return "0" + 5 # TypeError
    return 1/0 # ZeroDivisionError

def overtime_fn():
    while True:
        time.sleep(1)

def test_fn_registration_invalid():
    # Using a non-serialized payload data
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": "payload"})

    assert resp.status_code in [500, 400]

def test_fn_registration():
    # Using a real serialized function
    serialized_fn = serialize(double)
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialized_fn})

    assert resp.status_code in [200, 201]
    assert "function_id" in resp.json()


def test_execute_fn():
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": serialize(double)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((2,), {}))})

    print(resp)
    assert resp.status_code == 200 or resp.status_code == 201
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    resp = requests.get(f"{base_url}status/{task_id}")
    print(resp.json())
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id
    assert resp.json()["status"] in valid_statuses


def test_roundtrip():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code in [200, 201]
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):

        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        if resp.json()['status'] in ["COMPLETE", "FAILED"]:
            logging.warning(f"Task is now in {resp.json()['status']}")
            s_result = resp.json()
            logging.warning(s_result)
            result = deserialize(s_result['result'])
            assert result == number*2
            break
        time.sleep(0.01)

def test_execution_on_nonregistered_fn():
    non_existent_uuid = "11111111-1111-1111-1111-111111111111"
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": non_existent_uuid,
                               "payload": serialize(((2,), {}))})

    assert resp.status_code in [500, 400, 404]

def test_simple_success_task():    
    # Register function
    resp = requests.post(base_url + "register_function",
                         json={"name": "wait_fn",
                               "payload": serialize(wait_fn)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    # Create queues for storing status and result
    task_queue = Queue()
    result_queue = Queue()
    param = 3

    def fn():
        # Execute function
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((param,), {}))})
        task_queue.put(resp.json())

    def check_status():
        # Check status function
        task_info = task_queue.get()  
        task_id = task_info["task_id"]
        while True:
            resp = requests.get(f"{base_url}status/{task_id}")
            status = resp.json()["status"]
            result_queue.put(resp.json())
            if status in ["COMPLETE", "FAILED"]:
                break
            time.sleep(1)
        
    # Run threads: running_fn runs the function and check_status_fn checks the status asynchronously
    running_fn = threading.Thread(target=fn)
    check_status_fn = threading.Thread(target=check_status)
    running_fn.start()
    check_status_fn.start()
    running_fn.join()
    check_status_fn.join()
    
    # The result should be ordered - QUEUED...RUNNING...COMPLETE
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # Check intermediate results 
    for result in results[:-1]:
        assert result["status"] in ["QUEUED", "RUNNING"]

    # Check final result
    final_result = results[-1]
    assert final_result["status"] in ["COMPLETE"]
    
    # Check if result is correct
    result_task_id = final_result["task_id"]
    resp = requests.get(f"{base_url}result/{result_task_id}")
    assert resp.status_code == 200
    assert resp.json()["task_id"] == final_result["task_id"]
    s_result = resp.json()
    logging.warning(s_result)
    result = deserialize(s_result['result'])
    assert result == param * 2
    
def test_multiple_tasks():  
    # Register function
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    
    fn_info = resp.json()
    assert "function_id" in fn_info
    
    # Create multiple tasks
    num_tasks = 20
    # Use dummy uuids for task_ids
    task_ids = [""] * num_tasks 
    result = [0] * num_tasks
    async_run_threads = []
    async_gather_threads = []
    numbers = [random.randint(1, 100) for _ in range(num_tasks)]
    
    def async_run(num, i, task_ids):
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                 "payload": serialize(((num,), {}))})
        assert resp.status_code in [200, 201]
        task_ids[i] = resp.json()["task_id"]
    
    def async_gather(num, task_id, result, i):
        start_time = time.time()
        timeout = 30  
        while (time.time() - start_time) < timeout:
            try:
                resp = requests.get(f"{base_url}result/{task_id}")
                assert resp.status_code in [200, 201]
                status = resp.json()['status']
                if status == "COMPLETE":
                    res = deserialize(resp.json()['result'])
                    assert res == num * 2
                    result[i] = res
                    return
                elif status == "FAILED":
                    result[i] = -1
                    return
            except Exception as e:
                time.sleep(0.1) 
                continue
            time.sleep(0.1)
        result[i] = -2
    
    # Launch all tasks
    for i, num in enumerate(numbers):
        thread = threading.Thread(target=async_run, args=(num, i, task_ids))
        async_run_threads.append(thread)
        thread.start()   

    # Wait for all tasks to be created
    for thread in async_run_threads:
        thread.join()    
    
    # Verify all tasks got valid IDs
    assert all(task_id != "" for task_id in task_ids), "Not all tasks were assigned IDs"
    
    # Gather all results
    for i, task_id in enumerate(task_ids):
        thread = threading.Thread(target=async_gather, args=(numbers[i], task_id, result, i))
        async_gather_threads.append(thread)
        thread.start()
        
    # Wait for all results with timeout
    for thread in async_gather_threads:
        thread.join()
        
    # Verify results
    assert all(r != 0 for r in result), "Some tasks did not complete"
    for i, (num, res) in enumerate(zip(numbers, result)):
        assert res == num * 2, f"Task {i} returned incorrect result"

def test_simple_function_failure():
    # Register function
    resp = requests.post(base_url + "register_function",
                         json={"name": "wait_failed_fn",
                               "payload": serialize(wait_failed_fn)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    # Create queues for storing status and result
    task_queue = Queue()
    result_queue = Queue()
    param = 3

    def fn():
        # Execute function
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((param,), {}))})
        task_queue.put(resp.json())

    def check_status():
        # Check status function
        task_info = task_queue.get()  
        task_id = task_info["task_id"]
        while True:
            resp = requests.get(f"{base_url}status/{task_id}")
            status = resp.json()["status"]
            result_queue.put(resp.json())
            if status in ["COMPLETE", "FAILED"]:
                break
            time.sleep(1)
        
    # Run threads: running_fn runs the function and check_status_fn checks the status asynchronously
    running_fn = threading.Thread(target=fn)
    check_status_fn = threading.Thread(target=check_status)
    running_fn.start()
    check_status_fn.start()
    running_fn.join()
    check_status_fn.join()
    
    # The result should be ordered - QUEUED...RUNNING...FAILED
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # Check intermediate results 
    for result in results[:-1]:
        assert result["status"] in ["QUEUED", "RUNNING"]

    # Check final result
    final_result = results[-1]
    assert final_result["status"] in ["FAILED"]
    
    # Check if result is a proper failure message
    result_task_id = final_result["task_id"]
    resp = requests.get(f"{base_url}result/{result_task_id}")
    assert resp.status_code == 200
    assert resp.json()["task_id"] == final_result["task_id"]
    s_result = resp.json()
    logging.warning(s_result)
    result = deserialize(s_result['result'])
    assert result == "FailedFunctionError: Failed task execution due to division by zero" 

def test_simple_function_overtime_failure():
    # Register function
    resp = requests.post(base_url + "register_function",
                         json={"name": "overtime_fn",
                               "payload": serialize(overtime_fn)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    # Create queues for storing status and result
    task_queue = Queue()
    result_queue = Queue()

    def fn():
        # Execute function
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((), {}))})
        task_queue.put(resp.json())

    def check_status():
        # Check status function
        task_info = task_queue.get()  
        task_id = task_info["task_id"]
        for _ in range(200):
            resp = requests.get(f"{base_url}status/{task_id}")
            status = resp.json()["status"]
            result_queue.put(resp.json())
            if status in ["FAILED"]:
                break
            time.sleep(0.1)
        
    # Run threads: running_fn runs the function and check_status_fn checks the status asynchronously
    running_fn = threading.Thread(target=fn, daemon=True)  
    check_status_fn = threading.Thread(target=check_status)
    running_fn.start()
    check_status_fn.start()
    check_status_fn.join() 

    # The result should be ordered - QUEUED...RUNNING...FAILED
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # Check intermediate results 
    for result in results[:-1]:
        assert result["status"] in ["QUEUED", "RUNNING", "FAILED"]

    # Check final result
    final_result = results[-1]
    assert final_result["status"] in ["FAILED"]
    
    # Check if result is a proper failure message
    result_task_id = final_result["task_id"]
    resp = requests.get(f"{base_url}result/{result_task_id}")
    assert resp.status_code == 200
    assert resp.json()["task_id"] == final_result["task_id"]
    s_result = resp.json()
    logging.warning(s_result)
    result = deserialize(s_result['result'])
    assert result == "WorkerFailure: Task execution timed out"
    
    try:
        running_fn.join(timeout=1)  
    except TimeoutError:
        pass
