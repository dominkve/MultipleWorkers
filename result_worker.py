import redis

r = redis.Redis(host='172.30.221.102', port=6379, db=0)

result = r.brpop("result_queue")[1]

print(result)