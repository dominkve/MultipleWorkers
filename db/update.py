from pymongo import MongoClient
import json


# [*] Connect to MongoDB
client = MongoClient("mongodb://172.30.221.102:27017/")
db = client["test_db"]

problems_collection = db["problems"]
tests_collection = db["tests"]
execution_collection = db["executions"]

with open("problem.json", "r") as f:
    updated_data = json.load(f)


problems_collection.replace_one({"_id": "problem_1"}, updated_data)