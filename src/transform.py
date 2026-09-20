# Databricks notebook source
# Silver: unpack the JSON, fix types, keep the latest version of each reading, MERGE.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
SCHEMA = "weather_etl"
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_weather"
SILVER = f"{CATALOG}.{SCHEMA}.silver_weather"
PAYLOAD_SCHEMA = (
    "temperature_2m DOUBLE, relative_humidity_2m DOUBLE, "
    "precipitation DOUBLE, wind_speed_10m DOUBLE"
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER} (
  city STRING,
  obs_time TIMESTAMP,
  temperature_c DOUBLE,
  humidity_pct DOUBLE,
  precipitation_mm DOUBLE,
  wind_speed_kmh DOUBLE,
  ingested_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

parsed = (
    spark.table(BRONZE)
    .withColumn("p", F.from_json("payload", PAYLOAD_SCHEMA))
    .select(
        "city",
        F.col("obs_time").cast("timestamp").alias("obs_time"),
        F.col("p.temperature_2m").alias("temperature_c"),
        F.col("p.relative_humidity_2m").alias("humidity_pct"),
        F.col("p.precipitation").alias("precipitation_mm"),
        F.col("p.wind_speed_10m").alias("wind_speed_kmh"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

# If the same city + hour arrived in several batches, keep the newest one.
w = Window.partitionBy("city", "obs_time").orderBy(F.col("ingested_at").desc())
latest = parsed.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
latest.createOrReplaceTempView("silver_updates")

# COMMAND ----------

spark.sql(f"""
MERGE INTO {SILVER} AS t
USING silver_updates AS s
ON t.city = s.city AND t.obs_time = s.obs_time
WHEN MATCHED AND s.ingested_at > t.ingested_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Quality checks
silver = spark.table(SILVER)
total = silver.count()
assert total > 0, "Silver is empty"

dups = silver.groupBy("city", "obs_time").count().filter("count > 1").count()
assert dups == 0, f"{dups} duplicate city + hour keys in silver"

null_keys = silver.filter("city IS NULL OR obs_time IS NULL").count()
assert null_keys == 0, f"{null_keys} rows with a null city or time"

null_temp = silver.filter("temperature_c IS NULL").count()
assert null_temp / total < 0.05, f"{null_temp} of {total} rows have no temperature"

bad_temp = silver.filter("temperature_c < -90 OR temperature_c > 60").count()
assert bad_temp == 0, f"{bad_temp} rows with an impossible temperature"

print(f"Silver rows: {total}, all checks passed")