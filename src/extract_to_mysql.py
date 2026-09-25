import json
import os
import sys
from datetime import datetime, timezone

import mysql.connector
import requests
from dotenv import load_dotenv

load_dotenv()

CITIES = {
    "London": (51.5074, -0.1278),
    "New York": (40.7128, -74.0060),
    "Tokyo": (35.6762, 139.6503),
    "Sydney": (-33.8688, 151.2093),
    "Mumbai": (19.0760, 72.8777),
    "Kolkata": (22.5726, 88.3639),
}
API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]

UPSERT_SQL = """
INSERT INTO weather_raw (city, obs_time, payload)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE payload = VALUES(payload)
"""


def fetch_city(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY),
        "past_days": 1,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()["hourly"]


def to_rows(city, hourly):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i, t in enumerate(hourly["time"]):
        obs_time = datetime.strptime(t, "%Y-%m-%dT%H:%M")
        if obs_time > now:  # skip future hours (forecasts, not observations)
            continue
        payload = {key: hourly[key][i] for key in HOURLY}
        rows.append((city, obs_time, json.dumps(payload)))
    return rows


def connect():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "staging"),
    )


def main():
    conn = connect()
    cur = conn.cursor()
    failures = 0
    for city, (lat, lon) in CITIES.items():
        try:
            rows = to_rows(city, fetch_city(lat, lon))
            cur.executemany(UPSERT_SQL, rows)
            conn.commit()
            print(f"{city}: upserted {len(rows)} rows")
        except Exception as exc:
            failures += 1
            conn.rollback()
            print(f"{city}: FAILED ({exc})", file=sys.stderr)
    cur.close()
    conn.close()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()