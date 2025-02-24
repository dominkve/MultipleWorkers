import json
from pymongo import MongoClient

# [*] Connect to MongoDB
client = MongoClient("mongodb://172.30.221.102:27017/")
db = client["test_db"]

# Collections
problems_collection = db["problems"]
tests_collection = db["tests"]
execution_collection = db["executions"]

with open("problem.json", "r") as f:
    data = json.load(f)

data.pop("_id", None)

problems_collection.update_one(
    {"_id": "problem_1"},
    {"$set": data}
)

print("Data inserted sucessfully!")