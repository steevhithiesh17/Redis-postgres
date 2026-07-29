"""PostgreSQL connection management and write logic (the 'PostgreSQL' box)."""
import psycopg2
from datetime import datetime

from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

conn = None
cursor = None


def connect_postgres():
    """Open a PostgreSQL connection and cursor. Returns True on success."""
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
        print("PostgreSQL connection OK")
        return True
    except Exception as e:
        print("PostgreSQL connection failed:", e)
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
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ")
    except Exception as e:
        conn.rollback()
        print("Could not add timestamp columns:", e)


def write_to_postgres(data, received_at=None):
    """Insert/update a row based on data['table']. Raises on failure (caller handles retry)."""
    if conn is None or cursor is None:
        raise RuntimeError("PostgreSQL connection is not available")

    table = data.get("table")
    received_at_dt = received_at or datetime.now()
    updated_at_dt = datetime.now()

    if table in {"student", "employee"}:
        ensure_timestamp_columns(table)

    columns = get_table_columns(table)

    if table == "student":
        column_names = ["id", "name", "age", "department"]
        values = (
            int(data["id"]),
            data["name"],
            int(data["age"]),
            data["department"],
        )
        pk = "id"
    elif table == "employee":
        column_names = ["emp_id", "emp_name", "salary", "department"]
        values = (
            int(data["emp_id"]),
            data["emp_name"],
            float(data["salary"]),
            data["department"],
        )
        pk = "emp_id"
    else:
        raise ValueError(f"Unknown table: {table}")

    if "received_at" in columns:
        column_names.append("received_at")
        values = values + (received_at_dt,)
    if "updated_at" in columns:
        column_names.append("updated_at")
        values = values + (updated_at_dt,)

    set_clauses = [f"{c} = EXCLUDED.{c}" for c in column_names if c != pk]
    cursor.execute(
        f"""
        INSERT INTO {table} ({', '.join(column_names)})
        VALUES ({', '.join(['%s'] * len(column_names))})
        ON CONFLICT ({pk})
        DO UPDATE SET {', '.join(set_clauses)}
        """,
        values,
    )

    conn.commit()
    return updated_at_dt