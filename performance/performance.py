import subprocess
import time
import csv

client_program = "performance/client.py"
dispatcher_program = "task_dispatcher/task_dispatcher.py"
pull_worker_program = "workers/pull_worker.py"
push_worker_program = "workers/push_worker.py"
base_dispatcher_command = ["python", dispatcher_program]
dispatcher_port = "4567"  # change if needed
modes = ["local", "push", "pull"]
num_workers_list = [1, 2, 4, 8]
num_processors_list = [1, 2, 4, 8]
csv_files = {
    "pull": "performance/pull_results.csv",
    "push": "performance/push_results.csv",
    "local": "performance/local_results.csv"
}


def run_command(command, wait=False):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if wait:
        process.communicate()
    return process


def kill_all_processes(processes):
    for proc in processes:
        proc.terminate()
    for proc in processes:
        proc.wait()


def write_to_csv(filename, results):
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["num_worker", "num_processor", "time_taken"])
        writer.writerows(results)


def run_client_and_capture(num_tasks):
    start_time = time.time()
    client_command = ["python", client_program, str(num_tasks)]
    subprocess.run(client_command, check=True)
    return time.time() - start_time


def test_mode(mode, num_workers_list, num_processors_list):
    results = []
    worker_program = pull_worker_program if mode == "pull" else push_worker_program

    for num_workers in num_workers_list:
        for num_processors in num_processors_list:
            print(f"Testing {mode} mode with {num_workers} workers and {num_processors} processors per worker...")

            # Kill previous processes if any
            processes = []

            # Start dispatcher
            print("Starting dispatcher...")
            dispatcher_command = base_dispatcher_command + ["-m", mode, "-p", dispatcher_port]
            processes.append(run_command(dispatcher_command))

            # Start workers
            print(f"Starting {num_workers} workers...")
            for _ in range(num_workers):
                worker_command = ["python", worker_program, str(num_processors), f"tcp://127.0.0.1:{dispatcher_port}"]
                processes.append(run_command(worker_command))

            # Allow time for processes to start
            time.sleep(2)

            # Run client and measure time
            try:
                time_taken = run_client_and_capture(5 * num_workers)
                results.append([num_workers, num_processors, time_taken])
            except subprocess.CalledProcessError as e:
                print(f"Client execution failed: {e}")
                print("Skipping due to client failure.")
            finally:
                # Kill all processes
                print("Killing all processes...")
                kill_all_processes(processes)
                time.sleep(2)

    return results


def test_local_mode(num_workers_list):
    results = []

    for num_workers in num_workers_list:
        print(f"Testing local mode with {num_workers} workers...")

        # Kill previous processes if any
        processes = []

        # Start dispatcher
        print("Starting dispatcher in local mode...")
        dispatcher_command = base_dispatcher_command + ["-m", "local", "-w", str(num_workers)]
        processes.append(run_command(dispatcher_command))

        # Allow time for processes to start
        time.sleep(2)

        # Run client and measure time
        try:
            time_taken = run_client_and_capture(5 * num_workers)
            results.append([num_workers, "-", time_taken])
        except subprocess.CalledProcessError as e:
            print(f"Client execution failed: {e}")
            print("Skipping due to client failure.")
        finally:
            # Kill all processes
            print("Killing all processes...")
            kill_all_processes(processes)
            time.sleep(2)

    return results


def main():
    # Run tests for local mode
    print("Running tests for local mode...")
    results = test_local_mode(num_workers_list)
    write_to_csv(csv_files["local"], results)

    # Run tests for pull and push modes
    for mode in ["push", "pull"]:
        print(f"Running tests for {mode} mode...")
        results = test_mode(mode, num_workers_list, num_processors_list)
        write_to_csv(csv_files[mode], results)

    print("All tests completed. Results saved to CSV files.")


if __name__ == "__main__":
    main()
