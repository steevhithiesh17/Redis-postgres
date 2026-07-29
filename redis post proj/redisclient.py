"""Redis Stream connection, consumer group setup, and message reads."""
import redis

from config import REDIS_HOST, REDIS_PORT, STREAM, GROUP, CONSUMER, RETRY_IDLE_MS

r = None
_autoclaim_cursor = "0-0"


def connect_redis():
    global r
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        print("Redis connection OK")
        r = client
        return True
    except Exception as e:
        print("Redis connection failed:", e)
        r = None
        return False


def ensure_consumer_group():
    if r is None:
        return
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        print(f"Consumer group '{GROUP}' created on stream '{STREAM}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{GROUP}' already exists, continuing...")
        else:
            raise
    except Exception as e:
        print("Could not create consumer group:", e)


def read_messages():
    """Reclaim stale pending messages first and then read any new ones."""
    global _autoclaim_cursor
    if r is None:
        return [], []

    reclaimed = []
    try:
        _autoclaim_cursor, reclaimed, _deleted = r.xautoclaim(
            STREAM, GROUP, CONSUMER,
            min_idle_time=RETRY_IDLE_MS,
            start_id=_autoclaim_cursor,
            count=10,
        )
    except Exception as e:
        print("xautoclaim error:", e)
        reclaimed = []

    new_messages = []
    try:
        response = r.xreadgroup(
            groupname=GROUP,
            consumername=CONSUMER,
            streams={STREAM: ">"},
            count=10,
            block=1000,
        )
        if response:
            new_messages = response[0][1]
    except Exception as e:
        print("xreadgroup error:", e)
        new_messages = []

    return reclaimed + new_messages, reclaimed


def ack_and_delete(message_id):
    r.xack(STREAM, GROUP, message_id)
    r.xdel(STREAM, message_id)