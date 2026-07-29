"""Quick sanity check for Redis and PostgreSQL connectivity (replaces app.py / import redis.py)."""
import psycopg2
import redis

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, REDIS_HOST, REDIS_PORT

print("Checking PostgreSQL...")
try:
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM student")
    rows = cursor.fetchall()
    print(f"✅ Connected to PostgreSQL. {len(rows)} row(s) in student:")
    for row in rows:
        print(" ", row)
    cursor.close()
    conn.close()
except Exception as e:
    print("❌ PostgreSQL error:", e)

print("\nChecking Redis...")
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    print("✅ Redis ping:", r.ping())
except Exception as e:
    print("❌ Redis error:", e)