import json
from pymongo import MongoClient

# [*] Connect to MongoDB
client = MongoClient("mongodb://172.30.221.102:27017/")
db = client["test_db"]

# Collections
problems_collection = db["problems"]
tests_collection = db["tests"]
execution_collection = db["executions"]

problem = problems_collection.find_one({"_id": "problem_1"})

print("Problem:", problem)

with open("output.json", "w") as f:
    json.dump(problem, f, indent=2)
