# ==========================================================
#  ____    _    ____ _  _______ _   _ ____  
# | __ )  / \  / ___| |/ / ____| \ | |  _ \ 
# |  _ \ / _ \| |   | ' /|  _| |  \| | | | |
# | |_) / ___ \ |___| . \| |___| |\  | |_| |
# |____/_/   \_\____|_|\_\_____|_| \_|____/    
#
# ==========================================================
#  MODULE: Backend Producer
#  DESC:   Mimics backend behavior by queuing requests
#  AUTHOR: Dominkve
#  DATE:   2025-02-23
#  VER:    1.0
# ==========================================================
#  STRUCTURE:
#  - Generates simulated backend requests
#  - Pushes them into the Redis queue
#  - Mimics real API behavior for testing
# ==========================================================
#  USAGE:
#    python producer.py
# ==========================================================

# ----------------------------------------------------------

# ==========================================================
#  [SECTION] CONFIGURATION
# ==========================================================


# [*] Loading dependencies...
import redis
import uuid
import json

# [*] Connecting to broker...
r = redis.Redis(host='172.30.221.102', port=6379, db=0)

# [*] Defines the queue
queue = "request_queue"

# [*] Generating a unique id...
# [*] And defining all the request variables...
job_id = str(uuid.uuid4())
user_id = "alice123"
code = "def add(a, b):\n  return a + b"
language = "python"
mode = "test"
problem_id = "problem_1"


# ==========================================================
#  [END SECTION] CONFIGURATION
# ==========================================================

#-----------------------------------------------------------

# ==========================================================
#  [SECTION] SIMULATION
# ==========================================================


# [*] Emulates the request
# [*] The request should hold job_id, user_id, the code,
# [*] the language and the mode.
backend_request =  {
    "job_id": job_id,
    "user_id": user_id,
    "code": code,
    "language": language,
    "mode": mode,
    "problem_id": problem_id
}


# [*] Queueing a job...
r.lpush(queue, json.dumps(backend_request))

# [*] Debuging statement
from colorama import Fore, Style
print(f"{Fore.GREEN}# =========================================================")
print(f"Job {job_id} from {user_id} added to {queue}:")
print(f"# =========================================================")
print(f"language: {language}")
print(f"mode: {mode}")
print(f"code:\n{code}")
print(f"# ========================================================={Style.RESET_ALL}")



# ==========================================================
#  [END SECTION] SIMULATION
# ==========================================================

# -----------------------------------------------------------