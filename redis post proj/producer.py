"""
Example producer (Producer box). Pushes one message onto the Redis Stream
so you can watch listener.py pick it up, wait for execute_at, write it to
PostgreSQL, and send the success email.

Run listener.py in one terminal, then run this in another.
"""
from datetime import datetime, timedelta

import redis

from config import REDIS_HOST, REDIS_PORT, STREAM

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

message = {
    "table": "student",
    "id": "1",
    "name": "Ravi",
    "age": "22",
    "department": "CSE",
    "priority": "1",
    "execute_at": (datetime.now() + timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S"),
}

message_id = r.xadd(STREAM, message)
print("Queued message:", message_id, message)