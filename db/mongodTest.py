from pymongo import MongoClient

client = MongoClient("mongodb://172.30.221.102:27017/")
db = client["test_database"]

print(db.list_collection_names())  # Should list collections if connected
