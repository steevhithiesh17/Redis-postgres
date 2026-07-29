"""Message normalization, priority ordering, and 'is it due yet' logic (Scheduler box)."""
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
        text = value.strip().lower()
        if not text:
            return None
        if text in {"now", "immediate", "immediately", "today"}:
            return datetime.now()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(value.strip(), fmt)
            except ValueError:
                continue
        for fmt in ("%H:%M:%S", "%H:%M"):
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
    priority = data.get("priority")
    if priority is None:
        return 1
    try:
        return int(priority)
    except (TypeError, ValueError):
        return 1


def is_due(data):
    schedule_value = None
    for key in ("execute_at", "time", "scheduled_at", "run_at", "timestamp", "at"):
        if data.get(key):
            schedule_value = data.get(key)
            break

    if not schedule_value:
        return True

    scheduled_time = parse_schedule_value(schedule_value)
    if scheduled_time is None:
        return True

    return datetime.now() >= scheduled_time


def collect_ready_messages(messages):
    """Split raw stream messages into (ready, pending); ready is sorted by priority."""
    ready, pending = [], []
    for message_id, data in messages:
        normalized = normalize_data(data)
        (ready if is_due(normalized) else pending).append((message_id, normalized))
    ready.sort(key=lambda item: parse_priority(item[1]))
    return ready, pending