# =============================================================
# EXECUTION_WORKER
# =============================================================

# -------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------


# [*] Loading dependencies...
import redis
import json
import time
import docker
import tempfile
import os
import tarfile
from language_configs import LANGUAGE_CONFIGS

client = docker.from_env()
container = client.containers.run(
    "final",
    command="sh",
    detach=True,
    stdin_open=True,
    tty=True,
    mem_limit="256m",       # Memory limit (optional)
    cpu_period=100000,      # CPU throttling (optional)
    cpu_quota=50000,        # 50% CPU (optional)
    pids_limit=10
)


# [*] Connecting to broker...
r = redis.Redis(host='172.30.221.102', port=6379, db=0)

# [*] Queues
execution_queue = "execution_queue"
result_queue = "result_queue"


# --------------------------------------------------------------
# DEBUG
#---------------------------------------------------------------

# [*] Dependencies
from colorama import Fore

# [*] FUNCTION [*]
# Prints the job_data in a nice format
def pretty_job_data(job_data):
    job_id = job_data["job_id"]
    user_id = job_data["user_id"]
    code = job_data["code"]
    language = job_data["language"]
    mode = job_data["mode"]

    print(f"# =========================================================")
    print(f"Job {job_id} from {user_id} recieved from {execution_queue}:")
    print(f"# =========================================================")
    print(f"language: {language}")
    print(f"mode: {mode}")
    print(f"code:\n{code}")
    print(f"# =========================================================")

# --------------------------------------------------------------
# EXECUTION
# --------------------------------------------------------------

# [*] FUNCTION
# Runs code inside a container and returns the result
def run_code(code, language, container, mode):
    lang_cfg = LANGUAGE_CONFIGS[language]
    suffix = lang_cfg["suffix"]
    if (mode == "test"):
        arcname = f"script_test.{suffix}"
    else:
        arcname = f"script.{suffix}"


    # [*] Open a temp file
    # [*] It preserves idententation and is needed for compiled languages
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, mode="w") as temp_file:
        temp_file_path = temp_file.name
        temp_file.write(code)

    try:
        # Step 2: Create a tar archive
        tar_path = temp_file_path + ".tar"
        with tarfile.open(tar_path, "w") as tar:
            tar.add(temp_file_path, arcname=arcname)  # Ensure correct filename inside container

        # Step 3: Copy archive into the container
        with open(tar_path, "rb") as f:
            container.put_archive("/tmp", f.read())  # `/tmp/script.py` inside the container


        if lang_cfg["compile"]:
            compile_result = container.exec_run(lang_cfg["compile"])
            if compile_result.exit_code != 0:
                return {"result": f"Compilation failed.\n{compile_result.output.decode()}", "status": "failed"}
        
        exec_log = container.exec_run(lang_cfg[f"{mode}"])
        result = exec_log.output.decode()
        if exec_log.exit_code in [124, 137, 143]:  # 124 (timeout), 137 (SIGKILL), 143 (SIGTERM)
            result = "Error: Execution timed out after 5 seconds."
            status = "failed"

             # 🔥 Kill ALL stuck Python processes inside the container
            print("Killing stuck processes...")
            container.exec_run(lang_cfg["kill"])  # Force kill any lingering Python scripts


        elif exec_log.exit_code == 0:
            status = "completed"
        else:
            status = "failed"

        return {"status": status, "result": result}

    finally:
        os.remove(temp_file_path)
        os.remove(tar_path)


def is_container_healthy(container):
    """Check if the container is still responsive by running a simple command."""
    try:
        check = container.exec_run("echo test")  # Lightweight command
        return check.exit_code == 0  # If it succeeds, container is healthy
    except Exception as e:
        print(f"Container health check failed: {e}")
        return False  # If we can't even run 'echo', container is likely dead
    

def handle_job(job_data):
    print("Handling job...")

    if not is_container_healthy(container):
        print("Container is unresponsive. Restarting...")
        container.kill()
        container.start()

    output = run_code(job_data["code"], job_data["language"], container, job_data["mode"])
    status = output["status"]
    result = output["result"]
    result = {
        "job_id": job_data["job_id"],
        "result": result,
        "status": status,
    }
    r.lpush(result_queue, json.dumps(result))

    print(f"Job {job_data["job_id"]} completed with result: {output}")


# [*] FUNCTION
# Main function
def process_job():
    print(f"{Fore.GREEN}# =========================================================")
    print(f"Worker started, waiting for jobs...")

    while True:
        job_data = r.brpop(execution_queue)[1]
        if not job_data:
            time.sleep(1)
            continue
        
        job_data = json.loads(job_data)
        pretty_job_data(job_data)

        handle_job(job_data)
        print(f"# =========================================================")
        print()




if __name__ == "__main__":
    process_job()