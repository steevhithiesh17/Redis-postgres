import importlib.util
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import write_to_postgres
import listener


def test_student_payload_writes_expected_columns(monkeypatch):
    class DummyCursor:
        def __init__(self):
            self.executed = []

        def execute(self, query, params=None):
            self.executed.append((query, params))

        def fetchall(self):
            return [("id",), ("name",), ("age",), ("department",), ("received_at",), ("updated_at",)]

        def close(self):
            pass

    class DummyConn:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

        def close(self):
            pass

    dummy_conn = DummyConn()
    dummy_cursor = DummyCursor()

    monkeypatch.setattr("db.conn", dummy_conn)
    monkeypatch.setattr("db.cursor", dummy_cursor)

    data = {
        "table": "student",
        "id": "7",
        "name": "Ravi",
        "age": "22",
        "department": "CSE",
        "priority": "1",
    }

    write_to_postgres(data, received_at=None)

    assert dummy_conn.commits == 1
    assert any("INSERT INTO student" in query for query, _ in dummy_cursor.executed)
    assert any("received_at" in query for query, _ in dummy_cursor.executed)
    assert any("updated_at" in query for query, _ in dummy_cursor.executed)


def test_success_notifies_before_ack_and_delete(monkeypatch):
    events = []

    class DummyRedis:
        def xack(self, stream, group, message_id):
            events.append("xack")

        def xdel(self, stream, message_id):
            events.append("xdel")

    class DummyConn:
        def rollback(self):
            events.append("rollback")

    monkeypatch.setattr(listener, "r", DummyRedis())
    monkeypatch.setattr(listener, "conn", DummyConn())
    def write_successfully(data, received_at):
        events.append("write")
        return datetime.now()

    monkeypatch.setattr(listener, "write_to_postgres", write_successfully)
    monkeypatch.setattr(listener, "send_success_email", lambda data: events.append("email"))

    result = listener.handle_message("1-0", {"table": "student", "id": "1"})

    assert result == "processed"
    assert events == ["write", "email", "xack", "xdel"]


def test_database_failure_rolls_back_without_acknowledgement(monkeypatch):
    events = []

    class DummyRedis:
        def xack(self, stream, group, message_id):
            events.append("xack")

        def xdel(self, stream, message_id):
            events.append("xdel")

    class DummyConn:
        def rollback(self):
            events.append("rollback")

    def fail_write(data, received_at):
        events.append("write")
        raise ValueError("invalid payload")

    monkeypatch.setattr(listener, "r", DummyRedis())
    monkeypatch.setattr(listener, "conn", DummyConn())
    monkeypatch.setattr(listener, "write_to_postgres", fail_write)
    monkeypatch.setattr(listener, "send_success_email", lambda data: events.append("email"))

    result = listener.handle_message("1-0", {"table": "student", "id": "1"})

    assert result == "failed"
    assert events == ["write", "rollback"]
