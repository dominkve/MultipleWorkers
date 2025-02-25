import redis
import json
from pymongo import MongoClient

# [*] Connect to MongoDB
client = MongoClient("mongodb://172.30.221.102:27017/")
db = client["test_db"]

# [*] Load problem and test data
problems_collection = db["problems"]
tests_collection = db["tests"]

# [*] Connect to Redis
r = redis.Redis(host='172.30.221.102', port=6379, db=0)

job_data = r.brpop("test_queue")[1]
job_data = json.loads(job_data)

# ----------------------------------------------
# GENERATION
# ----------------------------------------------

# problem_data = problems_collection.find_one({"_id": job_data["problem_id"]})
test_data = tests_collection.find_one({"_id": job_data["language"]})
problem_data = problems_collection.find_one({"_id": job_data["problem_id"]})

framework = "pytest"
test_data = test_data["frameworks"][framework]

headers = test_data["headers"]
function = test_data["function"].format(FUNCTION_NAME=problem_data["FUNCTION_NAME"])
assertions = [
    test_data["assertion"].format(
            FUNCTION_NAME=problem_data["FUNCTION_NAME"],
            ASSERTION_PREFIX=test_case["ASSERTION_PREFIX"],
            INPUT=test_case["INPUT"],
            ASSERTION_OPERATOR=test_data["operators"][test_case["ASSERTION_OPERATOR"]],
            EXPECTED_OUTPUT=test_case["EXPECTED_OUTPUT"]
        )
        for test_case in problem_data["test_cases"]
]


test_code = headers + job_data["code"] + function + "".join(assertions)
print(test_code)
job_data["code"] = test_code
r.lpush("execution_queue", json.dumps(job_data))