#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

source .venv/Scripts/activate

echo "[1/4] Extracting from Open-Meteo..."
python src/extract_to_mysql.py

echo "[2/4] Sending new rows to Databricks..."
python src/mysql_to_volume.py

echo "[3/4] Running the Databricks pipeline job..."
python src/run_databricks_job.py

echo "[4/4] Pulling gold results back to MySQL..."
python src/gold_to_mysql.py

echo "Done."