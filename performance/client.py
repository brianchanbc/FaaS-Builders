# The client takes one argument, the number of tasks to execute. It registers a sleep function with the server, submits the tasks, and waits for all tasks to complete.
# It prints the time taken to complete all tasks.

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import time
import requests
from utils.serializer import serialize, deserialize

base_url = "http://127.0.0.1:8000/"

def sleep_function(x):
    import time
    time.sleep(3)
    return x

def main(n):
    # print("Registering the sleep function...")
    resp = requests.post(f"{base_url}register_function", json={"name": "sleep_function", "payload": serialize(sleep_function)})
    if resp.status_code not in [200, 201]:
        print("Failed to register function")
        return

    function_id = resp.json().get("function_id")
    if not function_id:
        print("Failed to retrieve function ID")
        return

    # print(f"Function registered with ID: {function_id}")

    # print(f"Submitting {n} tasks...")
    task_ids = []
    start_time = time.time()

    for i in range(n):
        resp = requests.post(f"{base_url}execute_function", json={"function_id": function_id, "payload": serialize(((i,), {}))})
        if resp.status_code in [200, 201]:
            task_id = resp.json().get("task_id")
            if task_id:
                task_ids.append(task_id)
        else:
            print(f"Failed to submit task {i}")

    # print(f"Submitted {len(task_ids)} tasks.")

    # wait for all tasks to complete
    # print("Waiting for tasks to complete...")
    completed_tasks = set()

    while len(completed_tasks) < len(task_ids):
        for task_id in task_ids:
            if task_id in completed_tasks:
                continue

            resp = requests.get(f"{base_url}result/{task_id}")
            if resp.status_code in [200, 201]:
                task_data = resp.json()
                if task_data["status"] == "COMPLETE":
                    completed_tasks.add(task_id)

        time.sleep(0.1)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"{elapsed_time:.4f}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client.py <number_of_tasks>")
        sys.exit(1)

    try:
        n = int(sys.argv[1])
        if n <= 0:
            raise ValueError
    except ValueError:
        print("Please provide a positive integer for the number of tasks.")
        sys.exit(1)

    main(n)