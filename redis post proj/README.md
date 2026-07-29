# Redis Stream Priority Scheduler

A small Python project that listens to a Redis Stream and schedules tasks into PostgreSQL based on a priority/execute_at schedule. After successful processing, the project sends a notification email.

Key behaviours:
- Read messages from a Redis stream (consumer group).
- Determine if a message is due (execute_at/time) and sort by priority.
- Write processed rows to PostgreSQL.
- Send a success email and acknowledge/delete the Redis message.
- Retry failed messages using Redis pending/xautoclaim semantics.

---

## Repository layout

- `listener.py` — main scheduler loop (Redis -> Scheduler -> PostgreSQL -> Email -> ACK/XDEL)
- `producer.py` — example producer to queue messages onto the Redis stream
- `redisclient.py` — Redis connection, consumer-group setup and read/ack helpers
- `db.py` — PostgreSQL connection and write logic
- `config.py` — configuration loader (uses `python-dotenv` / environment variables)
- `email service.py` — original email implementation (kept for compatibility)
- `email_service.py` — shim that imports `email service.py` (module name compatibility)
- `scheduler utilis.py` — scheduler helper functions (normalize, is_due, priority)
- `scheduler_utils.py` — shim that imports `scheduler utilis.py` (module name compatibility)
- `redis_client.py` — shim that imports `redisclient.py` (module name compatibility)
- `requirement.txt` — Python dependencies

---

## Prerequisites

- Python 3.11+ (project tested with Python 3.13)
- Docker (recommended) or a running Redis server
- PostgreSQL server accessible with credentials in the environment

Install Python dependencies:

```powershell
& "C:\\Path\\To\\python.exe" -m pip install -r requirement.txt
```

---

## Environment / Configuration

The project reads configuration from environment variables. You can create a `.env` file in the project root with values like:

```env
# PostgreSQL
DB_HOST=localhost
DB_NAME=tester
DB_USER=postgres
DB_PASSWORD=12345
DB_PORT=5432

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Stream / Group
REDIS_STREAM=schedule_stream
REDIS_GROUP=schedule_group
REDIS_CONSUMER=consumer1

# Email (Gmail example)
SENDER_EMAIL=you@gmail.com
APP_PASSWORD=your_app_password
RECEIVER_EMAIL=notify@domain.com

# Retry
RETRY_IDLE_MS=5000
```

`config.py` loads these via `python-dotenv` if present.

---

## Running (development)

Start Redis (Docker):

```powershell
docker run -d --name redis-dev -p 6379:6379 redis:6.2
```

Ensure PostgreSQL is running and the `DB_NAME` database exists and the `student`/`employee` tables exist (or let `db.py` add timestamp columns). Example quick SQL for `student`:

```sql
CREATE TABLE IF NOT EXISTS student (
  id integer PRIMARY KEY,
  name text,
  age integer,
  department text
);
```

Start the listener in one terminal:

```powershell
Set-Location 'C:\\Users\\Steev Hithiesh\\OneDrive\\Desktop\\redis post proj'
& "C:\\Users\\Steev Hithiesh\\AppData\\Local\\Programs\\Python\\Python313\\python.exe" listener.py
```

In another terminal queue a test message (the `producer.py` example queues with `execute_at` 10s in the future):

```powershell
& "C:\\Users\\Steev Hithiesh\\AppData\\Local\\Programs\\Python\\Python313\\python.exe" producer.py
```

Or push a message to run immediately:

```powershell
& "C:\\Users\\Steev Hithiesh\\AppData\\Local\\Programs\\Python\\Python313\\python.exe" -c "import redis,datetime; r=redis.Redis(host='localhost',port=6379,decode_responses=True); mid=r.xadd('schedule_stream', {'table':'student','id':'100','name':'Immediate','priority':'1','execute_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}); print('Added',mid)"
```

When a message is due the listener will log processing steps and attempt to send the configured success email.

Run `listener.py` as the consumer and add messages to Redis from a separate Redis client, such as `redis-cli` or another application:

```text
{"table":"student","id":"101","name":"Ravi","age":"22","department":"CSE","priority":"1"}
```

The listener reads the JSON fields from Redis and then processes the message. It does not publish records itself.

For example, with `redis-cli` running separately:

```powershell
redis-cli XADD schedule_stream * table student id 101 name Ravi age 22 department CSE priority 1
```

The `*` must be quoted in PowerShell if wildcard expansion causes a problem:

```powershell
redis-cli XADD schedule_stream '*' table student id 101 name Ravi age 22 department CSE priority 1
```

---

## Troubleshooting

- If you see `ModuleNotFoundError: No module named 'dotenv'` — install `python-dotenv` into the Python interpreter you run the project with.
- If you see `can't open file 'test'` when running `test connections.py`, quote the filename with spaces or rename the file. Example: `"test connections.py"`.
- If Redis connection fails with `unknown command 'HELLO'` that typically indicates an older redis-py client speaking a newer protocol or vice-versa; running Redis 6.2 (docker image used in dev) resolves that.
- Email issues: `Connection unexpectedly closed: [WinError 10054]` indicates the SMTP server closed the connection. Verify:
  - Your network allows outbound SMTP to Gmail (ports 587 or 465).
  - `SENDER_EMAIL` and `APP_PASSWORD` are correct (use App Password for Gmail, not your normal login password).
  - Try STARTTLS (port 587) first; the code now falls back to SSL on 465 if STARTTLS fails.

If email fails repeatedly you can disable `send_success_email` call in `listener.py` for testing database writes only.

---

## How the scheduler decides to run a message

- Message fields: `table`, `id` (or `emp_id`), columns for `student`/`employee`, `priority` (integer), and an optional `execute_at` (timestamp or time string).
- If `execute_at` (or other aliases like `time`, `scheduled_at`) is missing or unparsable, the message is considered due immediately.
- Ready messages are sorted by the `priority` integer (lower = higher priority).

---

## Notes about compatibility shims

Some files in the workspace had spaces or slightly different module names. To maintain the original import names used by `listener.py` and others, small shim modules were added:

- `redis_client.py` imports `redisclient.py`
- `email_service.py` imports `email service.py` (preserves the original file name)
- `scheduler_utils.py` imports `scheduler utilis.py`

These are thin wrappers so imports like `from email_service import send_success_email` continue to work.

---

If you want, I can:
- Add a simple `docker-compose.yml` to run Redis + PostgreSQL for development,
- Add unit tests for `scheduler utilis.py`,
- Or create a small CLI for pushing test messages.

Feel free to edit this README with your real credentials and any project-specific notes.
