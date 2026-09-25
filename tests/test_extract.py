import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from extract_to_mysql import to_rows, HOURLY


def make_hourly(times):
    return {
        "time": times,
        "temperature_2m": [20.0] * len(times),
        "relative_humidity_2m": [50.0] * len(times),
        "precipitation": [0.0] * len(times),
        "wind_speed_10m": [10.0] * len(times),
    }


def test_to_rows_skips_future_hours():
    now = datetime.utcnow()
    past = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
    future = (now + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M")
    rows = to_rows("TestCity", make_hourly([past, future]))
    assert len(rows) == 1
    assert rows[0][0] == "TestCity"


def test_to_rows_payload_has_expected_keys():
    now = datetime.utcnow()
    past = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
    rows = to_rows("TestCity", make_hourly([past]))
    import json
    payload = json.loads(rows[0][2])
    assert set(payload.keys()) == set(HOURLY)


def test_to_rows_empty_when_all_future():
    now = datetime.utcnow()
    future = (now + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M")
    rows = to_rows("TestCity", make_hourly([future]))
    assert rows == []