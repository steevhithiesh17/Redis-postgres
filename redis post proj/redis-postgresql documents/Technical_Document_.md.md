**TECHNICAL DOCUMENT**

**Reliable Event-Driven Data Processing System**

*Redis Streams • Python • PostgreSQL • SMTP Email Service*

Version 1.0

Companion Technical Implementation Reference to the System Design
Document

Table of Contents

1\. Introduction

This technical document describes the concrete implementation of the
Reliable Event-Driven Data Processing System introduced in the System
Design Document. Where the design document explains the intended
architecture at a conceptual level (Producer → Redis Stream → Python
Consumer → PostgreSQL → Email Notification), this document maps each of
those architectural components to the actual Python module(s) that
implement it, and includes the complete source code for each module.

The system is a small but complete asynchronous processing pipeline: a
producer publishes JSON-like messages to a Redis Stream; a long-running
listener process (the consumer) reads those messages through a Redis
Consumer Group, decides whether each message is due for execution and in
what priority order, writes the resulting row to PostgreSQL, sends a
confirmation email, and finally acknowledges and deletes the message
from the stream.

This document is intended for developers who need to install, run,
extend, or debug the system, and it should be read alongside the System
Design Document, which explains the reasoning behind the architecture.

2\. Technology Stack

  ------------------ ------------------------ ----------------------------
  **Layer**          **Technology**           **Purpose**

  Message Queue      Redis Streams            Durable, ordered queue with
                                              consumer groups,
                                              pending-entry tracking, and
                                              XAUTOCLAIM-based recovery

  Processing Layer   Python 3.11+             Consumer / scheduler logic,
                                              message normalization,
                                              orchestration

  Persistent Store   PostgreSQL               System of record for
                                              processed student / employee
                                              data

  Notifications      SMTP (Gmail)             Success email notifications
                                              sent via smtplib

  Driver Libraries   redis-py,                Redis client, PostgreSQL
                     psycopg2-binary,         client, environment/config
                     python-dotenv            loading
  ------------------ ------------------------ ----------------------------

3\. Repository Layout

The project consists of the following files. Several modules have a
"shim" counterpart whose only job is to re-export the real
implementation under an import-friendly name (the underlying files have
spaces in their names, which are not valid in a Python \`import\`
statement).

  -------------------- ---------------------------------------------------
  **File**             **Role**

  config.py            Central configuration loader (environment
                       variables + defaults)

  db.py                PostgreSQL connection management and write logic

  email service.py     Real email-sending implementation (filename
                       contains a space)

  email_service.py     Import shim that loads "email service.py" under the
                       name email_service

  scheduler utilis.py  Message normalization, due-time and priority logic
                       (filename contains a space)

  scheduler_utils.py   Import shim that loads "scheduler utilis.py" under
                       the name scheduler_utils

  redisclient.py       Reusable Redis connection / consumer-group / read /
                       ack helpers

  redis_client.py      Import shim: \`from redisclient import \*\`

  listener.py          Main scheduler / consumer process (self-contained;
                       embeds its own copies of the redis, DB and
                       scheduling helper logic)

  producer.py          Example producer that queues one test message onto
                       the Redis stream

  test connections.py  Standalone Redis + PostgreSQL connectivity smoke
                       test

  requirement.txt      Python dependency list

  README.md            Setup and run instructions
  -------------------- ---------------------------------------------------

> ***Note:** listener.py currently defines its own local copies of
> connect_redis, connect_postgres, normalize_data, is_due,
> parse_priority and write_to_postgres rather than importing them from
> redisclient.py, db.py and scheduler_utils.py. Functionally the logic
> is consistent across both copies today, but this duplication means a
> future bug fix or schema change must be applied in two places.
> Refactoring listener.py to import from the shared modules is
> recommended --- see Section 12, Observations & Recommendations.*

4\. Architecture-to-Code Mapping

The table below maps each architectural component named in the Design
Document to the Python module(s) that realize it.

  ---------------------------------- ------------------------------------
  **Design Document Component**      **Implementing Module(s)**

  Producer                           producer.py

  Redis Stream / Consumer Group      redisclient.py (helper module) and
                                     the equivalent inline logic in
                                     listener.py

  Python Consumer (validate →        listener.py (main loop,
  schedule → priority → write)       handle_message), scheduler utilis.py
                                     (normalize_data, is_due,
                                     parse_priority)

  PostgreSQL persistence             db.py (standalone module) and the
                                     equivalent inline logic in
                                     listener.py

  Email Notification Service         email service.py (implementation)
                                     via the email_service.py shim

  Configuration / environment        config.py
  variables                          

  Connectivity smoke test            test connections.py
  ---------------------------------- ------------------------------------

5\. Configuration Module --- config.py

config.py is the single source of configuration for every other module.
It loads variables from a .env file (via python-dotenv) if one is
present, then falls back to hard-coded defaults for local development.
It exposes PostgreSQL connection settings, Redis connection and
stream/group/consumer names, retry timing, and the SMTP credentials used
by the email service.

**config.py**

import os

from dotenv import load_dotenv

load_dotenv() \# loads variables from a .env file if present

\# PostgreSQL

DB_HOST = os.environ.get(\"DB_HOST\", \"localhost\")

DB_NAME = os.environ.get(\"DB_NAME\", \"tester\")

DB_USER = os.environ.get(\"DB_USER\", \"postgres\")

DB_PASSWORD = os.environ.get(\"DB_PASSWORD\", \"12345\")

DB_PORT = int(os.environ.get(\"DB_PORT\", 5432))

\# Redis

REDIS_HOST = os.environ.get(\"REDIS_HOST\", \"localhost\")

REDIS_PORT = int(os.environ.get(\"REDIS_PORT\", 6379))

STREAM = os.environ.get(\"REDIS_STREAM\", \"schedule_stream\")

GROUP = os.environ.get(\"REDIS_GROUP\", \"schedule_group\")

CONSUMER = os.environ.get(\"REDIS_CONSUMER\", \"consumer1\")

RETRY_IDLE_MS = int(os.environ.get(\"RETRY_IDLE_MS\", 5000))

\# Email (Gmail SMTP)

SENDER_EMAIL = os.environ.get(\"SENDER_EMAIL\",
\"steevhithiesh17x@gmail.com\")

APP_PASSWORD = os.environ.get(\"APP_PASSWORD\", \"ahhf qltp otct cmmf\")

RECEIVER_EMAIL = os.environ.get(\"RECEIVER_EMAIL\",
\"santhoshsteev1@gmail.com\")

> ***Note:** The defaults committed in this file include a real-looking
> database password and a Gmail App Password. Per Section 13 of the
> Design Document ("Do not hardcode passwords", "Use environment
> variables"), these values should be removed from source control,
> rotated, and supplied only via environment variables or a local .env
> file that is excluded via .gitignore.*

6\. Database Layer --- db.py

db.py owns the PostgreSQL connection (module-level conn / cursor
globals) and the write path for the two supported record types, student
and employee. Key responsibilities:

-   connect_postgres() / close_postgres() --- open and cleanly release
    the database connection.

-   get_table_columns() --- introspects information_schema.columns so
    the write logic can adapt to whichever optional columns exist on a
    given table.

-   ensure_timestamp_columns() --- defensively adds received_at and
    updated_at TIMESTAMPTZ columns to student / employee if they are
    missing, so the system can self-heal on first run.

-   write_to_postgres() --- builds and executes an INSERT ... ON
    CONFLICT (pk) DO UPDATE upsert for the target table, appending
    received_at / updated_at values only if those columns exist, then
    commits.

**db.py**

\"\"\"PostgreSQL connection management and write logic (the
\'PostgreSQL\' box).\"\"\"

import psycopg2

from datetime import datetime

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

conn = None

cursor = None

def connect_postgres():

\"\"\"Open a PostgreSQL connection and cursor. Returns True on
success.\"\"\"

global conn, cursor

try:

connection = psycopg2.connect(

host=DB_HOST,

database=DB_NAME,

user=DB_USER,

password=DB_PASSWORD,

port=DB_PORT,

)

connection.autocommit = False

conn = connection

cursor = connection.cursor()

print(\"PostgreSQL connection OK\")

return True

except Exception as e:

print(\"PostgreSQL connection failed:\", e)

conn = None

cursor = None

return False

def close_postgres():

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

def get_table_columns(table_name):

if cursor is None:

return set()

try:

cursor.execute(

\"\"\"

SELECT column_name

FROM information_schema.columns

WHERE table_schema = \'public\' AND table_name = %s

\"\"\",

(table_name,),

)

return {row\[0\].lower() for row in cursor.fetchall()}

except Exception:

return set()

def ensure_timestamp_columns(table_name):

if conn is None or cursor is None:

return

if table_name not in {\"student\", \"employee\"}:

return

try:

cursor.execute(f\"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS
received_at TIMESTAMPTZ\")

cursor.execute(f\"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS
updated_at TIMESTAMPTZ\")

except Exception as e:

conn.rollback()

print(\"Could not add timestamp columns:\", e)

def write_to_postgres(data, received_at=None):

\"\"\"Insert/update a row based on data\[\'table\'\]. Raises on failure
(caller handles retry).\"\"\"

if conn is None or cursor is None:

raise RuntimeError(\"PostgreSQL connection is not available\")

table = data.get(\"table\")

received_at_dt = received_at or datetime.now()

updated_at_dt = datetime.now()

if table in {\"student\", \"employee\"}:

ensure_timestamp_columns(table)

columns = get_table_columns(table)

if table == \"student\":

column_names = \[\"id\", \"name\", \"age\", \"department\"\]

values = (

int(data\[\"id\"\]),

data\[\"name\"\],

int(data\[\"age\"\]),

data\[\"department\"\],

)

pk = \"id\"

elif table == \"employee\":

column_names = \[\"emp_id\", \"emp_name\", \"salary\", \"department\"\]

values = (

int(data\[\"emp_id\"\]),

data\[\"emp_name\"\],

float(data\[\"salary\"\]),

data\[\"department\"\],

)

pk = \"emp_id\"

else:

raise ValueError(f\"Unknown table: {table}\")

if \"received_at\" in columns:

column_names.append(\"received_at\")

values = values + (received_at_dt,)

if \"updated_at\" in columns:

column_names.append(\"updated_at\")

values = values + (updated_at_dt,)

set_clauses = \[f\"{c} = EXCLUDED.{c}\" for c in column_names if c !=
pk\]

cursor.execute(

f\"\"\"

INSERT INTO {table} ({\', \'.join(column_names)})

VALUES ({\', \'.join(\[\'%s\'\] \* len(column_names))})

ON CONFLICT ({pk})

DO UPDATE SET {\', \'.join(set_clauses)}

\"\"\",

values,

)

conn.commit()

return updated_at_dt

7\. Redis Stream Client Layer --- redisclient.py / redis_client.py

redisclient.py centralizes all direct interaction with the Redis Stream:
connecting, creating the consumer group (idempotently, tolerating a
BUSYGROUP error if the group already exists), reading new and reclaimed
messages, and acknowledging/deleting processed messages. redis_client.py
is a one-line shim kept for import-name compatibility.

redisclient.py

**redisclient.py**

\"\"\"Redis Stream connection, consumer group setup, and message
reads.\"\"\"

import redis

from config import REDIS_HOST, REDIS_PORT, STREAM, GROUP, CONSUMER,
RETRY_IDLE_MS

r = None

\_autoclaim_cursor = \"0-0\"

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

print(\"Redis connection OK\")

r = client

return True

except Exception as e:

print(\"Redis connection failed:\", e)

r = None

return False

def ensure_consumer_group():

if r is None:

return

try:

r.xgroup_create(STREAM, GROUP, id=\"0\", mkstream=True)

print(f\"Consumer group \'{GROUP}\' created on stream \'{STREAM}\'\")

except redis.exceptions.ResponseError as e:

if \"BUSYGROUP\" in str(e):

print(f\"Consumer group \'{GROUP}\' already exists, continuing\...\")

else:

raise

except Exception as e:

print(\"Could not create consumer group:\", e)

def read_messages():

\"\"\"Reclaim stale pending messages first and then read any new
ones.\"\"\"

global \_autoclaim_cursor

if r is None:

return \[\], \[\]

reclaimed = \[\]

try:

\_autoclaim_cursor, reclaimed, \_deleted = r.xautoclaim(

STREAM, GROUP, CONSUMER,

min_idle_time=RETRY_IDLE_MS,

start_id=\_autoclaim_cursor,

count=10,

)

except Exception as e:

print(\"xautoclaim error:\", e)

reclaimed = \[\]

new_messages = \[\]

try:

response = r.xreadgroup(

groupname=GROUP,

consumername=CONSUMER,

streams={STREAM: \"\>\"},

count=10,

block=1000,

)

if response:

new_messages = response\[0\]\[1\]

except Exception as e:

print(\"xreadgroup error:\", e)

new_messages = \[\]

return reclaimed + new_messages, reclaimed

def ack_and_delete(message_id):

r.xack(STREAM, GROUP, message_id)

r.xdel(STREAM, message_id)

redis_client.py (compatibility shim)

**redis_client.py**

from redisclient import \*

read_messages() implements the reliability pattern from Section 11 of
the Design Document: it first calls XAUTOCLAIM to reclaim any messages
that have been pending (unacknowledged) for longer than RETRY_IDLE_MS
--- recovering work left behind by a crashed consumer --- and then calls
XREADGROUP to pull any brand-new messages. Both sets are returned
together, with the reclaimed set flagged separately.

8\. Scheduler Utilities --- scheduler utilis.py / scheduler_utils.py

This module contains the pure logic used to decide, for each incoming
message, whether it is normalized correctly, whether it is due to run
yet, and in what order due messages should be processed.

-   normalize_data() --- coerces whatever Redis hands back (dict, list
    of key/value pairs, bytes, or a bare string) into a clean {str:
    value} dictionary.

-   parse_schedule_value() --- parses a schedule field (numeric epoch,
    or one of several common date/time string formats, or keywords such
    as "now") into a datetime.

-   is_due() --- looks for a schedule field under any of several
    accepted aliases (execute_at, time, scheduled_at, run_at, timestamp,
    at) and compares it to the current time; a message with no
    recognizable schedule field is treated as due immediately.

-   parse_priority() --- reads the priority field as an integer,
    defaulting to 1 if it is missing or not a valid integer.

-   collect_ready_messages() --- splits a batch of raw stream messages
    into (ready, pending) and sorts the ready list by priority (lower
    value = higher priority), directly implementing the
    priority-ordering requirement in Section 6 of the Design Document.

scheduler utilis.py

**scheduler utilis.py**

\"\"\"Message normalization, priority ordering, and \'is it due yet\'
logic (Scheduler box).\"\"\"

from datetime import datetime

def normalize_data(data):

if isinstance(data, dict):

return {str(k).strip(): v for k, v in data.items()}

if isinstance(data, (list, tuple)):

normalized = {}

for item in data:

if isinstance(item, (list, tuple)) and len(item) == 2:

key, value = item

if isinstance(key, bytes):

key = key.decode(\"utf-8\")

if isinstance(value, bytes):

value = value.decode(\"utf-8\")

normalized\[str(key).strip()\] = value

return normalized

if isinstance(data, str):

return {\"value\": data}

return {}

def parse_schedule_value(value):

if value is None:

return None

if isinstance(value, (int, float)):

return datetime.fromtimestamp(float(value))

if isinstance(value, str):

text = value.strip().lower()

if not text:

return None

if text in {\"now\", \"immediate\", \"immediately\", \"today\"}:

return datetime.now()

for fmt in (\"%Y-%m-%d %H:%M:%S\", \"%Y-%m-%d %H:%M\",
\"%Y-%m-%dT%H:%M:%S\", \"%Y-%m-%dT%H:%M\"):

try:

return datetime.strptime(value.strip(), fmt)

except ValueError:

continue

for fmt in (\"%H:%M:%S\", \"%H:%M\"):

try:

parsed_time = datetime.strptime(value.strip(), fmt).time()

return datetime.combine(datetime.now().date(), parsed_time)

except ValueError:

continue

try:

return datetime.fromisoformat(value.strip())

except ValueError:

return None

return None

def parse_priority(data):

priority = data.get(\"priority\")

if priority is None:

return 1

try:

return int(priority)

except (TypeError, ValueError):

return 1

def is_due(data):

schedule_value = None

for key in (\"execute_at\", \"time\", \"scheduled_at\", \"run_at\",
\"timestamp\", \"at\"):

if data.get(key):

schedule_value = data.get(key)

break

if not schedule_value:

return True

scheduled_time = parse_schedule_value(schedule_value)

if scheduled_time is None:

return True

return datetime.now() \>= scheduled_time

def collect_ready_messages(messages):

\"\"\"Split raw stream messages into (ready, pending); ready is sorted
by priority.\"\"\"

ready, pending = \[\], \[\]

for message_id, data in messages:

normalized = normalize_data(data)

(ready if is_due(normalized) else pending).append((message_id,
normalized))

ready.sort(key=lambda item: parse_priority(item\[1\]))

return ready, pending

scheduler_utils.py (compatibility shim)

**scheduler_utils.py**

import importlib.util

import os

\_path = os.path.join(os.path.dirname(\_\_file\_\_), \"scheduler
utilis.py\")

spec = importlib.util.spec_from_file_location(\"scheduler_utils_impl\",
\_path)

\_mod = importlib.util.module_from_spec(spec)

spec.loader.exec_module(\_mod)

for \_name in dir(\_mod):

if not \_name.startswith(\"\_\"):

globals()\[\_name\] = getattr(\_mod, \_name)

> ***Note:** The default priority differs between this module (default
> 1, i.e. highest priority) and the equivalent function embedded inside
> listener.py (default 999, i.e. lowest priority). Since listener.py
> does not currently import from this module, the discrepancy has no
> effect today, but it should be resolved before the two code paths are
> merged.*

9\. Email Notification Service --- email service.py / email_service.py

The email service builds a plain-text confirmation email summarizing the
processed record and sends it through Gmail\'s SMTP servers. It first
attempts STARTTLS on port 587; if that fails for any reason it
automatically falls back to an implicit TLS connection (SMTP_SSL) on
port 465 before giving up and logging the error. As required by the
Design Document\'s success flow (Section 7), this function is only
called after the PostgreSQL write has already committed, and any
exception it raises is caught by the caller so that an email failure can
never trigger a duplicate database write or a retry.

email service.py (implementation)

**email service.py**

\"\"\"Email notification sent after a message is successfully processed
(Send Email box).\"\"\"

import smtplib

from email.message import EmailMessage

from datetime import datetime

from config import SENDER_EMAIL, APP_PASSWORD, RECEIVER_EMAIL

def send_success_email(data):

msg = EmailMessage()

msg\[\"Subject\"\] = \"Task Executed Successfully\"

msg\[\"From\"\] = SENDER_EMAIL

msg\[\"To\"\] = RECEIVER_EMAIL

table = data.get(\"table\")

body = f\"\"\"Hello,

Your scheduled task has been executed successfully.

Table : {table}

Execution Time : {datetime.now().strftime(\"%Y-%m-%d %H:%M:%S\")}

\"\"\"

if table == \"student\":

body += f\"\"\"

Student ID : {data.get(\'id\')}

Name : {data.get(\'name\')}

Age : {data.get(\'age\')}

Department : {data.get(\'department\')}

\"\"\"

elif table == \"employee\":

body += f\"\"\"

Employee ID : {data.get(\'emp_id\')}

Employee Name : {data.get(\'emp_name\')}

Salary : {data.get(\'salary\')}

Department : {data.get(\'department\')}

\"\"\"

body += \"\\nStatus : SUCCESS\\n\\nRegards,\\nRedis Scheduler\\n\"

msg.set_content(body)

def \_send_via_starttls():

with smtplib.SMTP(\"smtp.gmail.com\", 587, timeout=10) as smtp:

smtp.set_debuglevel(0)

smtp.ehlo()

smtp.starttls()

smtp.ehlo()

smtp.login(SENDER_EMAIL, APP_PASSWORD)

smtp.send_message(msg)

def \_send_via_ssl():

with smtplib.SMTP_SSL(\"smtp.gmail.com\", 465, timeout=10) as smtp:

smtp.set_debuglevel(0)

smtp.login(SENDER_EMAIL, APP_PASSWORD)

smtp.send_message(msg)

try:

try:

\_send_via_starttls()

print(\"Email Sent Successfully (STARTTLS)\")

return

except Exception as e_start:

print(\"Email Error (STARTTLS):\", e_start)

print(\"Retrying with SSL on port 465\...\")

try:

\_send_via_ssl()

print(\"Email Sent Successfully (SSL)\")

return

except Exception as e_ssl:

print(\"Email Error (SSL):\", e_ssl)

except Exception as e:

print(\"Email Error (unexpected):\", e)

email_service.py (compatibility shim)

**email_service.py**

import importlib.util

import os

\_path = os.path.join(os.path.dirname(\_\_file\_\_), \"email
service.py\")

spec = importlib.util.spec_from_file_location(\"email_service_impl\",
\_path)

email_service_impl = importlib.util.module_from_spec(spec)

spec.loader.exec_module(email_service_impl)

\# Re-export public symbols

for \_name in dir(email_service_impl):

if not \_name.startswith(\"\_\"):

globals()\[\_name\] = getattr(email_service_impl, \_name)

10\. Main Consumer / Scheduler --- listener.py

listener.py is the heart of the system --- the "Python Consumer"
described in Section 5.3 of the Design Document. It runs as a long-lived
process with a graceful-shutdown signal handler (SIGINT / SIGTERM) and
performs the following loop:

-   1\. Connect to Redis and PostgreSQL; create the consumer group if it
    does not already exist.

-   2\. Read due-for-retry (reclaimed) and new messages from the stream
    (read_messages()).

-   3\. Normalize each message and split it into ready vs. not-yet-due
    (collect_ready_messages()), sorting the ready set by priority.

-   4\. For every ready message, call handle_message(), which: writes to
    PostgreSQL, sends the success email as a best-effort side effect,
    and only then acknowledges (XACK) and deletes (XDEL) the message
    from the stream.

-   5\. Sleep briefly and repeat until shutdown is requested.

handle_message() is written so that failures are isolated to the correct
stage: a PostgreSQL write failure rolls back the transaction and leaves
the message un-acknowledged (so it will be retried / reclaimed later),
while an email failure after a successful commit is only logged --- it
can never cause the already-persisted record to be reprocessed or
double-emailed.

**listener.py**

import os

import signal

import time

from datetime import datetime

from email_service import send_success_email

import psycopg2

import redis

GROUP = \"schedule_group\"

CONSUMER = \"consumer1\"

STREAM = \"schedule_stream\"

RETRY_IDLE_MS = 5000

r = None

conn = None

cursor = None

shutdown_requested = False

autoclaim_cursor = \"0-0\"

def handle_shutdown(signum, frame):

global shutdown_requested

print(\"\\nShutdown requested. Stopping listener\...\")

shutdown_requested = True

signal.signal(signal.SIGINT, handle_shutdown)

signal.signal(signal.SIGTERM, handle_shutdown)

def connect_redis():

global r

try:

client = redis.Redis(

host=\"localhost\",

port=6379,

decode_responses=True,

socket_connect_timeout=2,

socket_timeout=2,

)

client.ping()

print(\"Redis connection OK\")

r = client

except Exception as e:

print(\"Redis connection failed:\", e)

r = None

def connect_postgres():

global conn, cursor

try:

connection = psycopg2.connect(

host=\"localhost\",

database=\"tester\",

user=\"postgres\",

password=\"12345\",

port=5432,

)

connection.autocommit = False

cursor = connection.cursor()

conn = connection

print(\"PostgreSQL connection OK\")

except Exception as e:

print(\"PostgreSQL connection failed:\", e)

conn = None

cursor = None

def ensure_consumer_group():

if r is None:

return

try:

r.xgroup_create(STREAM, GROUP, id=\"0\", mkstream=True)

print(f\"Consumer group \'{GROUP}\' created on stream \'{STREAM}\'\")

except redis.exceptions.ResponseError as e:

if \"BUSYGROUP\" in str(e):

print(f\"Consumer group \'{GROUP}\' already exists, continuing\...\")

else:

raise

except Exception as e:

print(\"Could not create consumer group:\", e)

def normalize_data(data):

if isinstance(data, dict):

return {str(k).strip(): v for k, v in data.items()}

if isinstance(data, (list, tuple)):

normalized = {}

for item in data:

if isinstance(item, (list, tuple)) and len(item) == 2:

key, value = item

if isinstance(key, bytes):

key = key.decode(\"utf-8\")

if isinstance(value, bytes):

value = value.decode(\"utf-8\")

normalized\[str(key).strip()\] = value

return normalized

if isinstance(data, str):

return {\"value\": data}

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

for fmt in (\"%Y-%m-%d %H:%M:%S\", \"%Y-%m-%d %H:%M\",
\"%Y-%m-%dT%H:%M:%S\", \"%Y-%m-%dT%H:%M\"):

try:

return datetime.strptime(text, fmt)

except ValueError:

continue

for fmt in (\"%H:%M:%S\", \"%H:%M\"):

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

priority = data.get(\"priority\")

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

\"\"\"

SELECT column_name

FROM information_schema.columns

WHERE table_schema = \'public\' AND table_name = %s

\"\"\",

(table_name,),

)

return {row\[0\].lower() for row in cursor.fetchall()}

except Exception:

return set()

def ensure_timestamp_columns(table_name):

if conn is None or cursor is None:

return

if table_name not in {\"student\", \"employee\"}:

return

try:

cursor.execute(

f\"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS received_at
TIMESTAMPTZ\"

)

cursor.execute(

f\"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS updated_at
TIMESTAMPTZ\"

)

conn.commit()

except Exception as e:

conn.rollback()

print(\"Could not add timestamp columns:\", e)

def is_due(data):

normalized_data = normalize_data(data)

schedule_value = None

for key in (\"execute_at\", \"time\", \"scheduled_at\", \"run_at\",
\"timestamp\", \"at\"):

if normalized_data.get(key):

schedule_value = normalized_data.get(key)

break

if not schedule_value:

return True

scheduled_time = parse_schedule_value(schedule_value)

if scheduled_time is None:

return True

current_time = datetime.now()

print(\"Current Time :\", current_time.strftime(\"%Y-%m-%d %H:%M:%S\"))

print(\"Execute Time :\", scheduled_time.strftime(\"%Y-%m-%d
%H:%M:%S\"))

return current_time \>= scheduled_time

def collect_ready_messages(messages):

ready = \[\]

pending = \[\]

for message_id, data in messages:

normalized_data = normalize_data(data)

if is_due(normalized_data):

ready.append((message_id, normalized_data))

else:

pending.append((message_id, normalized_data))

ready.sort(key=lambda item: parse_priority(item\[1\]))

return ready, pending

def write_to_postgres(data, received_at=None):

if conn is None or cursor is None:

raise RuntimeError(\"PostgreSQL connection is not available\")

normalized_data = normalize_data(data)

table = normalized_data.get(\"table\")

received_at_dt = received_at or datetime.now()

updated_at_dt = datetime.now()

if table in {\"student\", \"employee\"}:

ensure_timestamp_columns(table)

columns = get_table_columns(table)

if table == \"student\":

column_names = \[\"id\", \"name\", \"age\", \"department\"\]

values = (

int(normalized_data\[\"id\"\]),

normalized_data\[\"name\"\],

int(normalized_data\[\"age\"\]),

normalized_data\[\"department\"\],

)

if \"received_at\" in columns:

column_names.append(\"received_at\")

values = values + (received_at_dt,)

if \"updated_at\" in columns:

column_names.append(\"updated_at\")

values = values + (updated_at_dt,)

set_clauses = \[f\"{column} = EXCLUDED.{column}\" for column in
column_names if column != \"id\"\]

cursor.execute(

f\"\"\"

INSERT INTO student ({\', \'.join(column_names)})

VALUES ({\', \'.join(\[\'%s\'\] \* len(column_names))})

ON CONFLICT (id)

DO UPDATE SET

{\', \'.join(set_clauses)}

\"\"\",

values,

)

elif table == \"employee\":

column_names = \[\"emp_id\", \"emp_name\", \"salary\", \"department\"\]

values = (

int(normalized_data\[\"emp_id\"\]),

normalized_data\[\"emp_name\"\],

float(normalized_data\[\"salary\"\]),

normalized_data\[\"department\"\],

)

if \"received_at\" in columns:

column_names.append(\"received_at\")

values = values + (received_at_dt,)

if \"updated_at\" in columns:

column_names.append(\"updated_at\")

values = values + (updated_at_dt,)

set_clauses = \[f\"{column} = EXCLUDED.{column}\" for column in
column_names if column != \"emp_id\"\]

cursor.execute(

f\"\"\"

INSERT INTO employee ({\', \'.join(column_names)})

VALUES ({\', \'.join(\[\'%s\'\] \* len(column_names))})

ON CONFLICT (emp_id)

DO UPDATE SET

{\', \'.join(set_clauses)}

\"\"\",

values,

)

else:

raise ValueError(f\"Unknown table: {table}\")

conn.commit()

return updated_at_dt

def handle_message(message_id, data):

normalized_data = normalize_data(data)

received_at = datetime.now()

print(\"\\n====================================\")

print(\"Message ID :\", message_id)

print(\"Target Table :\", normalized_data.get(\"table\"))

print(\"Priority :\", parse_priority(normalized_data))

print(\"Received At :\", received_at.strftime(\"%Y-%m-%d %H:%M:%S\"))

print(\"Received Data :\", normalized_data)

if not normalized_data:

print(\"Message payload could not be parsed. Skipping.\")

return \"skipped\"

if not is_due(normalized_data):

print(\"Message is not due yet. Leaving it pending.\")

return \"pending\"

\#
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--

\# Step 1: the database write. This is the

\# part that determines whether the message

\# gets acked/retried - email is NOT allowed

\# to affect this outcome.

\#
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--

try:

print(\"Processing Started\")

start_time = datetime.now()

updated_at = write_to_postgres(normalized_data, received_at=received_at)

duration = datetime.now() - start_time

print(\"Processing Completed\")

print(\"Execution Duration :\", duration)

print(\"Updated In PostgreSQL At :\", updated_at.strftime(\"%Y-%m-%d
%H:%M:%S\"))

except Exception as e:

if conn is not None:

conn.rollback()

print(\"Processing Failed\")

print(\"Reason :\", e)

print(\"ACK Status : NOT SENT\")

print(\"Message will remain pending and be retried.\")

print(\"====================================\")

return \"failed\"

\#
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--

\# Step 2: best-effort notification. The

\# message is already committed and acked at

\# this point, so a failure here is just

\# logged - it must never cause a retry or

\# a duplicate email on the next pass.

\#
\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\--

try:

send_success_email(normalized_data)

except Exception as e:

print(\"Email Error (data already saved, not retrying because of
this):\", e)

try:

r.xack(STREAM, GROUP, message_id)

r.xdel(STREAM, message_id)

print(\"ACK Status : SENT\")

print(\"Message Deleted\")

except Exception as e:

print(\"ACK/Delete Error (message remains pending for retry):\", e)

print(\"ACK Status : NOT SENT\")

print(\"====================================\")

return \"failed\"

print(\"====================================\")

return \"processed\"

def read_messages():

global autoclaim_cursor

if r is None:

return \[\], \[\]

reclaimed = \[\]

try:

autoclaim_cursor, reclaimed, \_deleted = r.xautoclaim(

STREAM,

GROUP,

CONSUMER,

min_idle_time=RETRY_IDLE_MS,

start_id=autoclaim_cursor,

count=10,

)

except Exception as e:

print(\"xautoclaim error:\", e)

reclaimed = \[\]

new_messages = \[\]

try:

response = r.xreadgroup(

groupname=GROUP,

consumername=CONSUMER,

streams={STREAM: \"\>\"},

count=10,

block=1000,

)

if response:

new_messages = response\[0\]\[1\]

except Exception as e:

print(\"xreadgroup error:\", e)

new_messages = \[\]

combined = reclaimed + new_messages

return combined, reclaimed

def main():

print(\"=========================================\")

print(\" Redis Stream Priority Scheduler Started\")

print(\"=========================================\")

connect_redis()

connect_postgres()

ensure_consumer_group()

if r is None or conn is None or cursor is None:

print(\"Scheduler cannot start because Redis or PostgreSQL is
unavailable.\")

return

print(\"Listener is ready. Waiting for messages from Redis\...\")

try:

while not shutdown_requested:

messages, \_ = read_messages()

if messages:

print(\"Received Messages :\", messages)

ready_messages, pending_messages = collect_ready_messages(messages)

if ready_messages:

print(\"Ready Messages (sorted by priority) :\", ready_messages)

for message_id, data in ready_messages:

handle_message(message_id, data)

if pending_messages:

print(\"Pending due-to-future messages:\", pending_messages)

time.sleep(1)

except KeyboardInterrupt:

print(\"\\nShutting down scheduler\...\")

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

if \_\_name\_\_ == \"\_\_main\_\_\":

main()

11\. Producer and Connectivity Test

11.1 Producer --- producer.py

producer.py is the example "Producer" component from Section 5.1 of the
Design Document. It builds a single student message with an execute_at
timestamp ten seconds in the future and pushes it onto the Redis stream
using XADD (via redis-py\'s r.xadd), letting the operator watch
listener.py pick the message up, wait for the scheduled time, write it
to PostgreSQL, and send the confirmation email.

**producer.py**

\"\"\"

Example producer (Producer box). Pushes one message onto the Redis
Stream

so you can watch listener.py pick it up, wait for execute_at, write it
to

PostgreSQL, and send the success email.

Run listener.py in one terminal, then run this in another.

\"\"\"

from datetime import datetime, timedelta

import redis

from config import REDIS_HOST, REDIS_PORT, STREAM

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

message = {

\"table\": \"student\",

\"id\": \"1\",

\"name\": \"Ravi\",

\"age\": \"22\",

\"department\": \"CSE\",

\"priority\": \"1\",

\"execute_at\": (datetime.now() +
timedelta(seconds=10)).strftime(\"%Y-%m-%d %H:%M:%S\"),

}

message_id = r.xadd(STREAM, message)

print(\"Queued message:\", message_id, message)

11.2 Connectivity Smoke Test --- test connections.py

test connections.py is a standalone diagnostic script (note the space in
the filename --- it must be run with the filename quoted, e.g. python
\"test connections.py\"). It attempts to connect to PostgreSQL and print
the rows currently in the student table, then attempts to ping Redis,
printing a clear ✅ / ❌ status line for each check. It is useful for
confirming that config.py\'s connection settings are correct before
starting the full listener.

**test connections.py**

\"\"\"Quick sanity check for Redis and PostgreSQL connectivity (replaces
app.py / import redis.py).\"\"\"

import psycopg2

import redis

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT,
REDIS_HOST, REDIS_PORT

print(\"Checking PostgreSQL\...\")

try:

conn = psycopg2.connect(

host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
port=DB_PORT

)

cursor = conn.cursor()

cursor.execute(\"SELECT \* FROM student\")

rows = cursor.fetchall()

print(f\"✅ Connected to PostgreSQL. {len(rows)} row(s) in student:\")

for row in rows:

print(\" \", row)

cursor.close()

conn.close()

except Exception as e:

print(\"❌ PostgreSQL error:\", e)

print(\"\\nChecking Redis\...\")

try:

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

print(\"✅ Redis ping:\", r.ping())

except Exception as e:

print(\"❌ Redis error:\", e)

12\. Dependencies --- requirement.txt

The project\'s Python dependencies are pinned only by name (no version
pins), and are installed with pip install -r requirement.txt:

**requirement.txt**

psycopg2-binary

redis

python-dotenv

  ------------------ ------------------------------- --------------------
  **Package**        **Used by**                     **Purpose**

  psycopg2-binary    db.py, listener.py, test        PostgreSQL driver
                     connections.py                  

  redis              redisclient.py, listener.py,    Redis client
                     producer.py, test               (Streams, Consumer
                     connections.py                  Groups)

  python-dotenv      config.py                       Loads variables from
                                                     a local .env file
  ------------------ ------------------------------- --------------------

13\. Setup and Execution

These steps summarize the run instructions from README.md.

13.1 Install dependencies

pip install -r requirement.txt

13.2 Configure environment

Create a .env file in the project root (loaded automatically by
config.py via python-dotenv):

**.env**

DB_HOST=localhost

DB_NAME=tester

DB_USER=postgres

DB_PASSWORD=your_db_password

DB_PORT=5432

REDIS_HOST=localhost

REDIS_PORT=6379

REDIS_STREAM=schedule_stream

REDIS_GROUP=schedule_group

REDIS_CONSUMER=consumer1

SENDER_EMAIL=you@gmail.com

APP_PASSWORD=your_app_password

RECEIVER_EMAIL=notify@domain.com

RETRY_IDLE_MS=5000

13.3 Start Redis and ensure PostgreSQL tables exist

docker run -d \--name redis-dev -p 6379:6379 redis:6.2

CREATE TABLE IF NOT EXISTS student (

id integer PRIMARY KEY,

name text,

age integer,

department text

);

13.4 Run the listener, then the producer

python listener.py

\# in a second terminal

python producer.py

Or push a message directly with redis-cli:

redis-cli XADD schedule_stream \'\*\' table student id 101 name Ravi age
22 department CSE priority 1

14\. Security Considerations

Section 13 of the Design Document specifies a set of required security
practices. The table below records the current state of the
implementation against each requirement.

  ---------------------------------- ------------------------------------
  **Requirement (Design Document     **Current Status in Code**
  §13)**                             

  Do not hardcode passwords          Not met --- config.py ships
                                     real-looking default values for
                                     DB_PASSWORD and APP_PASSWORD

  Store credentials in environment   Partially met --- environment
  variables                          variables are supported and take
                                     precedence, but insecure defaults
                                     still exist

  Do not commit .env files to GitHub Cannot be verified from the supplied
                                     files; ensure .gitignore excludes
                                     .env

  Use SMTP App Passwords             Met --- email service.py
                                     authenticates with an App Password
                                     rather than the account password

  Restrict Redis / PostgreSQL access Not addressed in code --- an
  in production                      infrastructure/deployment concern

  Do not expose passwords in logs    Met --- no code path prints
                                     DB_PASSWORD or APP_PASSWORD

  Use TLS for production             Partially met --- SMTP uses
  communication where supported      STARTTLS/SSL; the Redis and
                                     PostgreSQL connections do not
                                     configure TLS
  ---------------------------------- ------------------------------------

> ***Note:** The single most important action item is to remove the
> hard-coded default DB_PASSWORD, APP_PASSWORD and email addresses from
> config.py, rotate the exposed Gmail App Password immediately, and
> require these values to be supplied only through the environment.*

15\. Differences from the Design Document

The Design Document describes several features that are not yet present
in the supplied source code. These are recorded here so that
documentation and implementation stay traceable to one another.

-   Dead Letter Queue (Sections 5.6, 8, 10): the design specifies a DLQ
    record containing the original message, retry count, error reason
    and failure timestamp after 3 failed attempts. The current
    listener.py has no MAX_RETRIES constant, no retry counter per
    message, and no DLQ stream or table --- a failed write is simply
    left un-acknowledged so Redis\'s pending-entries list will redeliver
    it indefinitely via XAUTOCLAIM.

-   Failure notification email (Section 5.5): only send_success_email()
    exists; there is no equivalent failure-notification function for a
    message that exhausts its retries.

-   Retry count tracking (Section 9): MAX_RETRIES = 3 is described in
    the design but is not implemented as a counter anywhere in the code.

None of this is a defect in the existing code --- what is implemented
(asynchronous processing, scheduled execution, priority ordering,
at-least-once delivery via consumer groups, success email, ack/delete)
works as documented. These items are natural next steps toward full
parity with the design.

16\. Observations and Recommendations

-   Consolidate duplicated logic: listener.py re-implements
    connect_redis, connect_postgres, normalize_data, is_due,
    parse_priority and write_to_postgres instead of importing them from
    redisclient.py, db.py and scheduler_utils.py. Importing the shared
    modules would remove the duplication risk noted in Section 3.

-   Implement the Dead Letter Queue and retry counter described in the
    Design Document (Sections 8--10) so failed messages are not retried
    forever.

-   Add a failure-notification email, matching the success path already
    implemented in email service.py.

-   Remove hard-coded secrets from config.py and rotate the exposed
    credentials (see Section 14).

-   Rename "email service.py", "scheduler utilis.py" and "test
    connections.py" to remove spaces (e.g. email_service_impl.py), which
    would let the shim modules be deleted entirely.

-   Pin dependency versions in requirement.txt for reproducible
    installs.

17\. Conclusion

The codebase implements the core event-driven pipeline described in the
Design Document: a decoupled Producer, a Redis Stream acting as a
durable queue with consumer-group semantics, a Python Consumer that
validates, schedules and prioritizes messages before writing them to
PostgreSQL, and a best-effort email notification step that cannot
interfere with the reliability of the database write or the message
acknowledgement. The main outstanding work, relative to the design, is
the Dead Letter Queue / retry-limit mechanism and consolidation of the
duplicated helper logic in listener.py.
