import io
import os
import sys
from datetime import datetime, timezone

import mysql.connector
import pandas as pd
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

PIPELINE = "weather_to_volume"
VOLUME_DIR = os.getenv(
    "DATABRICKS_VOLUME_PATH", "/Volumes/workspace/weather_etl/landing/weather"
)
EPOCH = datetime(1970, 1, 2)


def connect():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.getenv("MYSQL_DATABASE", "staging"),
    )


def get_watermark(cur):
    cur.execute(
        "SELECT last_loaded_at FROM etl_watermark WHERE pipeline_name = %s", (PIPELINE,)
    )
    row = cur.fetchone()
    return row[0] if row else EPOCH


def set_watermark(cur, value):
    cur.execute(
        """
        INSERT INTO etl_watermark (pipeline_name, last_loaded_at)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_loaded_at = VALUES(last_loaded_at)
        """,
        (PIPELINE, value),
    )


def main():
    conn = connect()
    cur = conn.cursor()

    watermark = get_watermark(cur)
    cur.execute(
        """
        SELECT city, obs_time, payload, ingested_at
        FROM weather_raw
        WHERE ingested_at > %s
        ORDER BY ingested_at
        """,
        (watermark,),
    )
    rows = cur.fetchall()
    if not rows:
        print("No new rows since watermark", watermark)
        return

    cols = [c[0] for c in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    df["payload"] = df["payload"].apply(
        lambda v: v.decode() if isinstance(v, (bytes, bytearray)) else v
    )
    new_watermark = df["ingested_at"].max()

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    target = f"{VOLUME_DIR}/batch_{stamp}.parquet"

    w = WorkspaceClient()
    w.files.upload(target, buf, overwrite=True)

    set_watermark(cur, new_watermark)
    conn.commit()
    print(f"Uploaded {len(df)} rows to {target}; watermark is now {new_watermark}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)