import os
import sys
import time

import mysql.connector
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv

load_dotenv()

GOLD = os.getenv("DATABRICKS_GOLD_TABLE", "workspace.weather_etl.gold_daily_weather")
COLS = [
    "city", "obs_date", "avg_temp_c", "min_temp_c", "max_temp_c",
    "avg_humidity_pct", "total_precip_mm", "avg_wind_kmh", "readings",
]

DDL = """
CREATE TABLE IF NOT EXISTS gold_daily_weather (
  city             VARCHAR(64) NOT NULL,
  obs_date         DATE        NOT NULL,
  avg_temp_c       DOUBLE,
  min_temp_c       DOUBLE,
  max_temp_c       DOUBLE,
  avg_humidity_pct DOUBLE,
  total_precip_mm  DOUBLE,
  avg_wind_kmh     DOUBLE,
  readings         INT,
  loaded_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (city, obs_date)
)
"""

UPSERT = (
    f"INSERT INTO gold_daily_weather ({', '.join(COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(COLS))}) "
    "ON DUPLICATE KEY UPDATE "
    + ", ".join(f"{c} = VALUES({c})" for c in COLS if c not in ("city", "obs_date"))
)


def connect():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database="serving",
    )


def run_query(w, warehouse_id, sql):
    res = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=sql, wait_timeout="30s"
    )
    deadline = time.time() + 300
    while res.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.time() > deadline:
            raise TimeoutError("Databricks query took longer than 5 minutes")
        time.sleep(3)
        res = w.statement_execution.get_statement(res.statement_id)
    if res.status.state != StatementState.SUCCEEDED:
        detail = res.status.error.message if res.status.error else ""
        raise RuntimeError(f"Databricks query {res.status.state}: {detail}")
    return res


def convert(value, type_name):
    if value is None:
        return None
    t = str(getattr(type_name, "value", type_name)).upper()
    if t in ("DOUBLE", "FLOAT", "DECIMAL"):
        return float(value)
    if t in ("INT", "LONG", "SHORT", "BYTE"):
        return int(value)
    return value


def main():
    w = WorkspaceClient()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        warehouse_id = next(iter(w.warehouses.list())).id

    res = run_query(w, warehouse_id, f"SELECT {', '.join(COLS)} FROM {GOLD}")
    types = [c.type_name for c in res.manifest.schema.columns]
    raw_rows = res.result.data_array or []
    rows = [tuple(convert(v, t) for v, t in zip(r, types)) for r in raw_rows]
    if not rows:
        raise RuntimeError("Gold table returned no rows; nothing to load")

    conn = connect()
    cur = conn.cursor()
    cur.execute(DDL)
    cur.executemany(UPSERT, rows)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM gold_daily_weather")
    total = cur.fetchone()[0]
    print(f"Loaded {len(rows)} rows from Databricks; serving.gold_daily_weather now has {total} rows")
    cur.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)