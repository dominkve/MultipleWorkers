import pymongo.errors
import redis
import os
import time
import uuid
import logging
import sys
import random
import pymongo
import json
import traceback
from functools import wraps

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "test_db")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)

class Worker:
    def __init__(self, mongo_uri=MONGO_URI, db=MONGO_DB, redis_host=REDIS_HOST, redis_port=REDIS_PORT, stream="tasks_group", 
                 group="workers", pending_check_interval=10, min_idle_time=15000,
                 base_delay=500, max_delay=6000
                ):
        self.mongo_uri = mongo_uri
        self.db_name = db
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.stream = stream
        self.group = group
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.last_id = '0-0'
        self.check_backlog = True
        self.PENDING_CHECK_INTERVAL = pending_check_interval
        self.iterations_since_pending_check = 0
        self.MIN_IDLE_TIME = min_idle_time
        self.BASE_DELAY = base_delay
        self.MAX_DELAY = max_delay
        self.delay = self.BASE_DELAY
        self.redis_client = self.connect_with_retries(self.connect_redis, "Redis")
        self.mongo_client = self.connect_with_retries(self.connect_mongo, "MongoDB")

        try:
            self.init_collections()

        except pymongo.errors.ConnectionFailure as e:
            logging.warning(f"Lost connection to MongoDB: {e}. Reconnecting...")
            self.mongo_client = self.connect_with_retries(self.connect_mongo, "MongoDB")
            if self.mongo_client:
                try:
                    self.init_collections()
                except Exception as e:
                    logging.error(f"Failed to reinitialize database collections: {e}")

        except pymongo.errors.ServerSelectionTimeoutError as e:
            logging.warning(f"Cannot connect to MongoDB: {e}. Reconnecting...")
            self.mongo_client = self.connect_with_retries(self.connect_mongo, "MongoDB")
            if self.mongo_client:
                try:
                    self.init_collections()
                except Exception as e:
                    logging.error(f"Failed to reinitialize database collections: {e}")

        except pymongo.errors.OperationFailure as e:
            # Currently there is no auth in the system
            logging.error(f"Failed MongoDB authentication: {e}")
        except pymongo.errors.InvalidName as e:
            logging.error(f"Invalid collection or database name: {e}")
        except Exception as e:
            logging.warning(f"Unexpected setup error... {e}.")

    def connect_with_retries(self, connect_func, service_name, base_delay=1, max_delay=10, max_attempts=5):
        """Attempts to connect to Redis with exponential backoff."""
        attempts = 0
        current_delay = base_delay

        while attempts < max_attempts:
            try:
                return connect_func()
            except Exception as e:
                logging.warning(f"{service_name} connection failed: {e}. Retrying in {current_delay} seconds...")
                time.sleep(current_delay + random.uniform(0, 1))  # Jitter to avoid simultaneous retries
                current_delay = min(current_delay * 2, max_delay)  # Exponential backoff with cap
                attempts += 1

        logging.error(f"Critical failure: Could not connect to {service_name} after {max_attempts} attempts. Exiting...")
        sys.exit(1)

    def connect_redis(self):
        client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True,
                             socket_connect_timeout=3, socket_timeout=5
                             )
        client.ping()  # Ensure connection is active
        logging.info("Connected to Redis.")
        return client

    def connect_mongo(self):
        client = pymongo.MongoClient(self.mongo_uri, serverSelectionTimeoutMS=10000, socketTimeoutMs=5000)
        client.admin.command("ping")  # Ensure connection is active
        logging.info("Connected to MongoDB.")
        return client
    
    def init_collections(self):
        db=self.mongo_client[self.db_name]
        self.problems = db["problems"]
        self.tests = db["tests"]

    def process_input(self, input_value):
        logging.info("processing input")
        try:
            parsed_value = json.loads(input_value)
            return parsed_value if isinstance(parsed_value, (list, dict)) else input_value
        except json.JSONDecodeError:
            return input_value  # It's a primitive type

    def build_harness(self, job_data):
        logging.info("Building harness")

        problem_data = self.problems.find_one({"_id": job_data["problem_id"]})
        test_data = self.tests.find_one({"_id": job_data["language"]})

        headers = test_data["headers"]
        logging.info(headers)        
        function = test_data["function"].format(FUNCTION_NAME=problem_data["FUNCTION_NAME"])
        logging.info(function)
        assertions = []
        for test_case in problem_data["test_cases"]:
            input_value = self.process_input(test_case["INPUT"])
            expected_value = self.process_input(test_case["EXPECTED_OUTPUT"])
            assertion = test_data["assertion"].format(
                    FUNCTION_NAME=problem_data["FUNCTION_NAME"],
                    ASSERTION_PREFIX=test_case["ASSERTION_PREFIX"],
                    INPUT=input_value,
                    ASSERTION_OPERATOR=test_data["operators"][test_case["ASSERTION_OPERATOR"]],
                    EXPECTED_OUTPUT=expected_value
                )
            
            assertions.append(assertion)
        
        main = test_data["main"]

        test_code = headers + job_data["code"] + function + "".join(assertions) + main
        logging.info(test_code)
        job_data["code"] = test_code

        return job_data

    def process_message(self, data):
        """ Simulated workload processing """
        logging.info(f"Processing data... {data}")
        if not data or "json" not in data:
            return {"direction": "DLQ_STREAM", "error": "Missing JSON payload"}
        
        job_payload = data["json"]
        try:
            job_data = json.loads(job_payload)
        except json.JSONDecodeError as e:
            logging.warning(f"Received invalid JSON, skipping... {e}")
            return {"direction": "DLQ_STREAM", "error": e}
        
        test_harness = self.build_harness(job_data)
        return {"direction": "execution_stream", "json": json.dumps(test_harness)}
        
    def fetch_pending_messages(self):
        """ Uses XAUTOCLAIM to fetch and reassign stuck messages """
        try:
            logging.debug("Trying to fetch pending messages...")
            new_last_id, messages, _ = self.redis_client.xautoclaim(
                self.stream, self.group, self.worker_id, self.MIN_IDLE_TIME, self.last_id, count=5
            )

            if messages:
                logging.debug(f"{self.worker_id} auto-claimed {len(messages)} messages.")
                if new_last_id:
                    self.last_id = new_last_id # Update last_id if needed
            else:
                logging.debug("No pending messages to claim.")

            return messages
        
        except redis.exceptions.ConnectionError as e:
            logging.warning(f"Lost connection to Redis: {e}. Reconnecting...")
            self.redis_client = self.connect_with_retries(self.connect_redis, "Redis")
        except redis.RedisError as e:
            logging.warning(f"Redis error: {e}")
            time.sleep(0.5)
        except Exception as e:
            logging.warning(f"Unexpected error: {e}")
            time.sleep(0.5)
        
        return []

    def run(self):
        """ Main event loop for processing messages """
        while True:
            try:
                # Occasionally check for pending messages
                if self.check_backlog:
                    pending_messages = self.fetch_pending_messages()
                    for message_id, data in pending_messages:
                        result = self.process_message(data)
                        if result["direction"] == "DLQ_STREAM":
                            self.redis_client.xack(self.stream, self.group, message_id)
                            self.redis_client.xadd(result["direction"], {"error": result["error"]})
                        elif result["direction"]:
                            logging.info(f"Going to {result["direction"]}")
                            self.redis_client.xack(self.stream, self.group, message_id)
                            self.redis_client.xadd(result["direction"], {"json": result["json"]})

                    self.check_backlog = bool(pending_messages)

                self.iterations_since_pending_check += 1
                if self.iterations_since_pending_check % self.PENDING_CHECK_INTERVAL == 0:
                    self.check_backlog = True
                    self.iterations_since_pending_check = 0

                # Read new messages (fallback to pending if necessary)
                myid = self.last_id if self.check_backlog else ">"
                messages = self.redis_client.xreadgroup(
                    self.group, self.worker_id, streams={self.stream: myid}, count=1, block=self.delay
                )
                
                self.delay = self.BASE_DELAY if messages else min(int(self.delay * 1.5), self.MAX_DELAY)

                if messages:
                    for stream, entries in messages:
                        for message_id, data in entries:
                            result = self.process_message(data)
                            if result["direction"] == "DLQ_STREAM":
                                self.redis_client.xack(self.stream, self.group, message_id)
                                self.redis_client.xadd(result["direction"], {"error": result["error"]})
                                self.last_id = message_id  # Update last processed message ID
                            elif result["direction"]:
                                logging.info(f"Going to {result["direction"]}")
                                self.redis_client.xack(self.stream, self.group, message_id)
                                self.redis_client.xadd(result["direction"], {"json": result["json"]})
                                self.last_id = message_id  # Update last processed message ID
            
            except redis.exceptions.ConnectionError as e:
                logging.warning(f"Lost connection: {e}. Reconnecting...")
                self.redis_client = self.connect_with_retries(self.connect_redis, "Redis")
            except redis.RedisError as e:
                logging.warning(f"Redis error: {e}")
                time.sleep(0.5)
            except Exception as e:
                logging.warning(f"Unexpected error: {e}")
                time.sleep(0.5)

if __name__ == "__main__":
    worker = Worker(mongo_uri=MONGO_URI, db=MONGO_DB, redis_host=REDIS_HOST, redis_port=REDIS_PORT,
                    stream="test_stream", group="test_workers")
    worker.run()
