import os
import signal
import time
from datetime import datetime
from email_service import send_success_email

import psycopg2
import redis


GROUP = "schedule_group"
CONSUMER = "consumer1"
STREAM = "schedule_stream"
RETRY_IDLE_MS = 5000

r = None
conn = None
cursor = None

shutdown_requested = False
autoclaim_cursor = "0-0"


def handle_shutdown(signum, frame):
    global shutdown_requested
    print("\nShutdown requested. Stopping listener...")
    shutdown_requested = True


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def connect_redis():
    global r
    try:
        client = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
        print("Redis connection OK")
        r = client
    except Exception as e:
        print("Redis connection failed:", e)
        r = None


def connect_postgres():
    global conn, cursor
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="tester",
            user="postgres",
            password="12345",
            port=5432,
        )
        connection.autocommit = False
        cursor = connection.cursor()
        conn = connection
        print("PostgreSQL connection OK")
    except Exception as e:
        print("PostgreSQL connection failed:", e)
        conn = None
        cursor = None


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


def normalize_data(data):
    if isinstance(data, dict):
        return {str(k).strip(): v for k, v in data.items()}

    if isinstance(data, (list, tuple)):
        normalized = {}
        for item in data:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                key, value = item
                if isinstance(key, bytes):
                    key = key.decode("utf-8")
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                normalized[str(key).strip()] = value
        return normalized

    if isinstance(data, str):
        return {"value": data}

    return {}


def parse_schedule_value(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value))

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                parsed_time = datetime.strptime(text, fmt).time()
                return datetime.combine(datetime.now().date(), parsed_time)
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    return None


def parse_priority(data):
    priority = data.get("priority")
    if priority is None:
        return 999
    try:
        return int(priority)
    except (TypeError, ValueError):
        return 999


def get_table_columns(table_name):
    if cursor is None:
        return set()

    try:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return {row[0].lower() for row in cursor.fetchall()}
    except Exception:
        return set()


def ensure_timestamp_columns(table_name):
    if conn is None or cursor is None:
        return

    if table_name not in {"student", "employee"}:
        return

    try:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ"
        )
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ"
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("Could not add timestamp columns:", e)


def is_due(data):
    normalized_data = normalize_data(data)
    schedule_value = None

    for key in ("execute_at", "time", "scheduled_at", "run_at", "timestamp", "at"):
        if normalized_data.get(key):
            schedule_value = normalized_data.get(key)
            break

    if not schedule_value:
        return True

    scheduled_time = parse_schedule_value(schedule_value)
    if scheduled_time is None:
        return True

    current_time = datetime.now()
    print("Current Time :", current_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("Execute Time :", scheduled_time.strftime("%Y-%m-%d %H:%M:%S"))
    return current_time >= scheduled_time


def collect_ready_messages(messages):
    ready = []
    pending = []

    for message_id, data in messages:
        normalized_data = normalize_data(data)
        if is_due(normalized_data):
            ready.append((message_id, normalized_data))
        else:
            pending.append((message_id, normalized_data))

    ready.sort(key=lambda item: parse_priority(item[1]))
    return ready, pending


def write_to_postgres(data, received_at=None):
    if conn is None or cursor is None:
        raise RuntimeError("PostgreSQL connection is not available")

    normalized_data = normalize_data(data)
    table = normalized_data.get("table")
    received_at_dt = received_at or datetime.now()
    updated_at_dt = datetime.now()

    if table in {"student", "employee"}:
        ensure_timestamp_columns(table)

    columns = get_table_columns(table)

    if table == "student":
        column_names = ["id", "name", "age", "department"]
        values = (
            int(normalized_data["id"]),
            normalized_data["name"],
            int(normalized_data["age"]),
            normalized_data["department"],
        )

        if "received_at" in columns:
            column_names.append("received_at")
            values = values + (received_at_dt,)
        if "updated_at" in columns:
            column_names.append("updated_at")
            values = values + (updated_at_dt,)

        set_clauses = [f"{column} = EXCLUDED.{column}" for column in column_names if column != "id"]
        cursor.execute(
            f"""
            INSERT INTO student ({', '.join(column_names)})
            VALUES ({', '.join(['%s'] * len(column_names))})
            ON CONFLICT (id)
            DO UPDATE SET
                {', '.join(set_clauses)}
            """,
            values,
        )
    elif table == "employee":
        column_names = ["emp_id", "emp_name", "salary", "department"]
        values = (
            int(normalized_data["emp_id"]),
            normalized_data["emp_name"],
            float(normalized_data["salary"]),
            normalized_data["department"],
        )

        if "received_at" in columns:
            column_names.append("received_at")
            values = values + (received_at_dt,)
        if "updated_at" in columns:
            column_names.append("updated_at")
            values = values + (updated_at_dt,)

        set_clauses = [f"{column} = EXCLUDED.{column}" for column in column_names if column != "emp_id"]
        cursor.execute(
            f"""
            INSERT INTO employee ({', '.join(column_names)})
            VALUES ({', '.join(['%s'] * len(column_names))})
            ON CONFLICT (emp_id)
            DO UPDATE SET
                {', '.join(set_clauses)}
            """,
            values,
        )
    else:
        raise ValueError(f"Unknown table: {table}")

    conn.commit()
    return updated_at_dt


def handle_message(message_id, data):
    normalized_data = normalize_data(data)
    received_at = datetime.now()
    print("\n====================================")
    print("Message ID :", message_id)
    print("Target Table :", normalized_data.get("table"))
    print("Priority :", parse_priority(normalized_data))
    print("Received At :", received_at.strftime("%Y-%m-%d %H:%M:%S"))
    print("Received Data :", normalized_data)

    if not normalized_data:
        print("Message payload could not be parsed. Skipping.")
        return "skipped"

    if not is_due(normalized_data):
        print("Message is not due yet. Leaving it pending.")
        return "pending"

    # -----------------------------------------
    # Step 1: the database write. This is the
    # part that determines whether the message
    # gets acked/retried - email is NOT allowed
    # to affect this outcome.
    # -----------------------------------------
    try:
        print("Processing Started")
        start_time = datetime.now()
        updated_at = write_to_postgres(normalized_data, received_at=received_at)
        duration = datetime.now() - start_time
        print("Processing Completed")
        print("Execution Duration :", duration)
        print("Updated In PostgreSQL At :", updated_at.strftime("%Y-%m-%d %H:%M:%S"))

    except Exception as e:
        if conn is not None:
            conn.rollback()
        print("Processing Failed")
        print("Reason :", e)
        print("ACK Status : NOT SENT")
        print("Message will remain pending and be retried.")
        print("====================================")
        return "failed"

    # -----------------------------------------
    # Step 2: best-effort notification. The
    # message is already committed and acked at
    # this point, so a failure here is just
    # logged - it must never cause a retry or
    # a duplicate email on the next pass.
    # -----------------------------------------
    try:
        send_success_email(normalized_data)
    except Exception as e:
        print("Email Error (data already saved, not retrying because of this):", e)

    try:
        r.xack(STREAM, GROUP, message_id)
        r.xdel(STREAM, message_id)
        print("ACK Status : SENT")
        print("Message Deleted")
    except Exception as e:
        print("ACK/Delete Error (message remains pending for retry):", e)
        print("ACK Status : NOT SENT")
        print("====================================")
        return "failed"

    print("====================================")
    return "processed"


def read_messages():
    global autoclaim_cursor
    if r is None:
        return [], []

    reclaimed = []
    try:
        autoclaim_cursor, reclaimed, _deleted = r.xautoclaim(
            STREAM,
            GROUP,
            CONSUMER,
            min_idle_time=RETRY_IDLE_MS,
            start_id=autoclaim_cursor,
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

    combined = reclaimed + new_messages
    return combined, reclaimed


def main():
    print("=========================================")
    print(" Redis Stream Priority Scheduler Started")
    print("=========================================")

    connect_redis()
    connect_postgres()
    ensure_consumer_group()

    if r is None or conn is None or cursor is None:
        print("Scheduler cannot start because Redis or PostgreSQL is unavailable.")
        return

    print("Listener is ready. Waiting for messages from Redis...")

    try:
        while not shutdown_requested:
            messages, _ = read_messages()
            if messages:
                print("Received Messages :", messages)

            ready_messages, pending_messages = collect_ready_messages(messages)
            if ready_messages:
                print("Ready Messages (sorted by priority) :", ready_messages)

            for message_id, data in ready_messages:
                handle_message(message_id, data)

            if pending_messages:
                print("Pending due-to-future messages:", pending_messages)

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down scheduler...")
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()