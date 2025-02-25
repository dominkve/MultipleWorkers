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
from colorama import Fore, Style
from pymongo import MongoClient

# [*] Connect to MongoDB
client = MongoClient("mongodb://172.30.221.102:27017/")
db = client["test_db"]

# [*] Load problem and test data
problems_collection = db["problems"]
languages_collection = db["languages"]

# [*] Connecting to broker...
try:
    r = redis.Redis(host='172.30.221.102', port=6379, db=0)
    r.ping() # Test connection
    print(f"{Fore.GREEN}[✓] Connected to Redis broker.{Style.RESET_ALL}")
except:
    print(F"{Fore.RED}[✗] Failed to connect to Redis.{Style.RESET_ALL}")
    exit(1)

# [*] Queues
request_queue = "request_queue"
execution_queue = "execution_queue"
test_queue = "test_queue"
result_queue = "result_queue"

# --------------------------------------------------------------
# DEBUG
#---------------------------------------------------------------

# [*] FUNCTION [*]
def pretty_request(job_data):
    """Prints the request in a nicely formatted way."""
    print(f"{Fore.CYAN}# =========================================================")
    print(f"Job {job_data.get('job_id', 'unknown')} from {job_data.get('user_id', 'unknown')} received:")
    print(f"# =========================================================")
    print(f"Language: {job_data.get('language', 'unknown')}")
    print(f"Mode: {job_data.get('mode', 'unknown')}")
    print(f"Code:\n{job_data.get('code', '[No code provided]')}")
    print(f"# ========================================================={Style.RESET_ALL}")
    
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
    
        # Validate job_id type
    if not isinstance(job_data["job_id"], str):
        return "job_id must be a string."
    
    # [*] Validate mode
    if job_data["mode"] not in {"run", "test"}:
        return "Invalid mode"
    
    # Validate problem_id only if mode is "test"
    if job_data["mode"] == "test":
        if "problem_id" not in job_data or not isinstance(job_data["problem_id"], str):
            return "Invalid or missing problem_id."

        problem = problems_collection.find_one(
            {"_id": job_data["problem_id"], "allowed_languages": job_data["language"]}
        )
        
        if not problem:
            return "Invalid problem or language not allowed"

    # [*] Validate language
    if not languages_collection.find_one({"_id": job_data["language"]}):
        return "Unsupported language..."
    
    # [*] Validate code
    if job_data["code"] == "":
        return "There is no code..."
    
    # Validate code length (e.g., limit to 100KB)
    if len(job_data["code"]) > 100_000:
        return "Code size exceeds limit."
    
    return None


# [*] FUNCTION [*]
# Main function, processes requests, passes them on to the
# right queue, based on validity and mode
def process_request():
    """Main worker function that processes incoming requests."""
    print(f"{Fore.BLUE}[*] Request worker started, processing requests...{Style.RESET_ALL}")

    while True:
        try:
            job_data = r.brpop(request_queue, timeout=5)
            if job_data is None:  # Fix: Avoid accessing NoneType
                continue  # No job, retry
            
            _, job_payload = job_data  # Extract JSON payload
            try:
                job_data = json.loads(job_payload)
            except json.JSONDecodeError:
                print(f"{Fore.RED}[!] Received invalid JSON, skipping...{Style.RESET_ALL}")
                continue

            pretty_request(job_data)
            error_message = validate_request(job_data)

            if error_message:
                print(f"{Fore.RED}[-] Validation failed: {error_message}{Style.RESET_ALL}")
                r.lpush(result_queue, json.dumps({
                    "job_id": job_data.get("job_id", "unknown"),
                    "status": "failed",
                    "result": error_message
                }))
            else:
                target_queue = execution_queue if job_data["mode"] == "run" else test_queue
                r.lpush(target_queue, json.dumps(job_data))
                print(f"{Fore.GREEN}[+] Job pushed to {target_queue}{Style.RESET_ALL}")

            print()
        
        except redis.ConnectionError:
            print(f"{Fore.RED}[✗] Lost connection to Redis, retrying in 5 seconds...{Style.RESET_ALL}")
            time.sleep(5)
        except Exception as e:
            print(f"{Fore.RED}[!] Unexpected error: {e}{Style.RESET_ALL}")
            time.sleep(1)  # Prevent spamming in case of rapid failures



# [*] Ensures this worker runs standalone
if __name__ == "__main__":
    process_request()