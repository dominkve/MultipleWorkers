# =============================================================
# REQUEST_WORKER
# =============================================================

# -------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------

# [*] Dependencies
import redis
import time
import json
from language_configs import LANGUAGE_CONFIGS

# [*] Connecting to broker...
r = redis.Redis(host='172.30.221.102', port=6379, db=0)

# [*] Queues
request_queue = "request_queue"
execution_queue = "execution_queue"
test_queue = "test_queue"
result_queue = "result_queue"

# --------------------------------------------------------------
# DEBUG
#---------------------------------------------------------------

# [*] Dependencies
from colorama import Fore

# [*] FUNCTION [*]
# Prints the request in a nice format
def pretty_request(job_data):
    job_id = job_data["job_id"]
    user_id = job_data["user_id"]
    code = job_data["code"]
    language = job_data["language"]
    mode = job_data["mode"]

    print(f"# =========================================================")
    print(f"Job {job_id} from {user_id} recieved from {request_queue}:")
    print(f"# =========================================================")
    print(f"language: {language}")
    print(f"mode: {mode}")
    print(f"code:\n{code}")
    print(f"# =========================================================")
    
# --------------------------------------------------------------
# PROCESSING
# --------------------------------------------------------------

# [*] FUNCTION [*]
# Validates requests, checks for job id, langauge, mode and code
def validate_request(job_data):
    """Basic validation of job request."""
    required_fields = {"job_id", "mode", "code", "language"}

    # [*] Ensure all required fields exist
    if not all(field in job_data for field in required_fields):
        return "Missing required fields."
    
    # [*] Validate mode
    if job_data["mode"] not in {"run", "test"}:
        return "Invalid mode"
    
    # [*] Validate language
    if job_data["language"] not in LANGUAGE_CONFIGS:
        return f"Unsupported language: {job_data['language']}"
    

    return None


# [*] FUNCTION [*]
# Main function, processes requests, passes them on to the
# right queue, based on validity and mode
def process_request():
    print(f"{Fore.BLUE}# =========================================================")
    print("Request_worker started, processing requests...")

    while True:
        job_data = r.brpop(request_queue)[1]
        if not job_data:
            time.sleep(1)
            continue
        
        job_data = json.loads(job_data)

        pretty_request(job_data)

        error_message = validate_request(job_data)

        if error_message:
            print(f"{error_message}")
            print(f"# =========================================================")
            r.lpush(result_queue, json.dumps({
                "job_id": job_data.get("job_id", "unknown"),
                "status": "failed",
                "result": error_message
            }))
        else:
            if job_data["mode"] == "run":
                print(f"Pushed job to {execution_queue}")
                print(f"# =========================================================")
                r.lpush(execution_queue, json.dumps(job_data))
            elif job_data["mode"] == "test":
                print(f"Pushed job to {test_queue}")
                print(f"# =========================================================")
                r.lpush(test_queue, json.dumps(job_data))
        print()



# [*] Ensures this worker can only exist on its own
if __name__ == "__main__":
    process_request()