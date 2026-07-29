import os
from dotenv import load_dotenv

load_dotenv()  # loads variables from a .env file if present

# PostgreSQL
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME", "tester")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "12345")
DB_PORT = int(os.environ.get("DB_PORT", 5432))

# Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

STREAM = os.environ.get("REDIS_STREAM", "schedule_stream")
GROUP = os.environ.get("REDIS_GROUP", "schedule_group")
CONSUMER = os.environ.get("REDIS_CONSUMER", "consumer1")
RETRY_IDLE_MS = int(os.environ.get("RETRY_IDLE_MS", 5000))

# Email (Gmail SMTP)
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "steevhithiesh17x@gmail.com")
APP_PASSWORD = os.environ.get("APP_PASSWORD", "ahhf qltp otct cmmf")
RECEIVER_EMAIL = os.environ.get("RECEIVER_EMAIL", "santhoshsteev1@gmail.com")