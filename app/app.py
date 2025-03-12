from flask import Flask, jsonify, request
from flask_cors import CORS
import redis
import json
import uuid
import time
import pymongo
import os

MONGO_URI = os.getenv("MONGO_URI", "idk")

app = Flask(__name__)
CORS(app) # Allow requests from any origin

r = redis.Redis(host='172.30.221.102', port=6379, db=0)
m = pymongo.MongoClient(MONGO_URI)

db = m["test_db"]
problems = db["problems"]


@app.route("/problem", methods=["POST"])
def problem():
    data = request.get_json()
    problem_id = data["problem_id"]

    problem_data = problems.find_one({"_id": problem_id})
    print(f"Problem data: {problem_data}")

    return jsonify({
        "title": problem_data.get("title", "Untitled"),
        "desc": problem_data.get("description", "No description available"),
        "function_name": problem_data.get("FUNCTION_NAME", "Anything"),
        "allowed_languages": problem_data.get("allowed_languages", "Anything")
    })

def submit_job(code, mode, language, problem_id):
    job_id = str(uuid.uuid4())
    job = {"job_id": job_id, "language": language, "mode": mode, "code": code, "problem_id": problem_id}
    print(f"Adding job to queue: {job}")
    # Send job to the queue
    r.xadd("request_stream", {"json": json.dumps(job)})

    return job_id

@app.route("/submit", methods=["POST"])
def submit():
    data = request.get_json()
    job_id = submit_job(data["code"], data["mode"], data["language"], data["problem_id"])
    return jsonify({"job_id": job_id})

#--------------------------------------------------

@app.route("/result", methods=["POST"])
def get_job_status():
    data = request.get_json()
    job_id = data.get("job_id")

    if not job_id:
        return jsonify({"error": "Missing job_id"}), 400
    
    start_time = time.time()
    timeout = 7

    while time.time() - start_time < timeout:
        
        job_status = r.hget(f"job:{job_id}", "status")
        job_result = r.hget(f"job:{job_id}", "output")

        if job_status:
            job_status = job_status.decode()
            if job_status == "completed":
                return jsonify({"status": f"{job_status}", "output": job_result.decode() if job_result else None})
            elif job_status == "failed":
                return jsonify({"status": f"{job_status}", "output": job_result.decode() if job_result else None})
        
        time.sleep(0.5) 


    return jsonify({"status": "pending", "output": None})

if __name__ == "__main__":
    app.run(debug=True)