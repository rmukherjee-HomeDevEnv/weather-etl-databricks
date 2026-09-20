# Databricks notebook source
# Gold: one row per city per day, ready for dashboards.

CATALOG = "workspace"
SCHEMA = "weather_etl"
SILVER = f"{CATALOG}.{SCHEMA}.silver_weather"
GOLD = f"{CATALOG}.{SCHEMA}.gold_daily_weather"

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD} AS
SELECT
  city,
  DATE(obs_time) AS obs_date,
  ROUND(AVG(temperature_c), 2) AS avg_temp_c,
  MIN(temperature_c) AS min_temp_c,
  MAX(temperature_c) AS max_temp_c,
  ROUND(AVG(humidity_pct), 1) AS avg_humidity_pct,
  ROUND(SUM(precipitation_mm), 2) AS total_precip_mm,
  ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_kmh,
  COUNT(*) AS readings
FROM {SILVER}
GROUP BY city, DATE(obs_time)
""")

# COMMAND ----------

# Quality checks
gold = spark.table(GOLD)
assert gold.count() > 0, "Gold is empty"
too_many = gold.filter("readings > 24").count()
assert too_many == 0, f"{too_many} city-days have more than 24 hourly readings"
print(f"Gold rows: {gold.count()}, all checks passed")
display(gold.orderBy("city", "obs_date"))