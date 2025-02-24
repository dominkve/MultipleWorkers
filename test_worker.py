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

problem_data = problems_collection.find_one({"_id": job_data["problem_id"]})
test_data = tests_collection.find_one({"_id": job_data["language"]})

def headers(problem_data, test_data):
    headers = set(test_data["test_headers"])
    for tag in problem_data["function_headers"]:
        if tag in test_data["header_mappings"]:
            headers.add(test_data["header_mappings"][tag])

    header_string = "\n".join(headers)

    print(header_string)
    return header_string

def test_harness(problem_data, test_data):
    test_assertions = ""
    function_name = problem_data["function_name"]
    test_function = test_data["test_function"]
    test_function = test_function.replace("{function_name}", function_name)
    test_cases = problem_data["test_cases"]
    test_assertion = test_data["test_assertions"]
    for test_case in test_cases:
        test_assertion = test_data["test_assertions"]
        input = test_case['input']
        expected_output = test_case['expected_output']

        test_assertion = test_assertion.replace("{input}", input)
        test_assertion = test_assertion.replace("{expected_output}", str(expected_output))

        test_assertion = test_assertion.replace("{function_name}", function_name)
        print(test_assertion)
        test_assertions += test_assertion
    
    print(test_assertions)
    test_harness = test_function + test_assertions
    print(test_harness)
    return test_harness

    


header = headers(problem_data, test_data)
harness = test_harness(problem_data, test_data)

test = header + '\n' + job_data["code"] + '\n\n' + harness
print()
print(test)

job_data["code"] = test
print()
print(job_data["code"])
r.lpush("execution_queue", json.dumps(job_data))