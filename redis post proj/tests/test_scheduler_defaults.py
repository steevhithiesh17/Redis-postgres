from scheduler_utils import is_due, parse_priority


def test_missing_priority_and_schedule_use_defaults():
    data = {"table": "student", "id": "1", "name": "Ravi"}

    assert parse_priority(data) == 1
    assert is_due(data) is True
