# Databricks notebook source
# Bronze: load new Parquet files from the landing volume into a Delta table, untouched.

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "weather_etl"
SRC = f"/Volumes/{CATALOG}/{SCHEMA}/landing/weather"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/landing/_checkpoints/bronze"
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_weather"

# COMMAND ----------

# Auto Loader remembers which files it has read, so each file is loaded once.
query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
    .load(SRC)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_loaded_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", f"{CHECKPOINT}/state")
    .trigger(availableNow=True)
    .toTable(BRONZE)
)
query.awaitTermination()

# COMMAND ----------

# Quality check: bronze must not be empty
n = spark.table(BRONZE).count()
assert n > 0, "Bronze is empty: no files were loaded from the landing volume"
print(f"Bronze rows: {n}")