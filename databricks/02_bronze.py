# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: raw → Delta (append-only)
# MAGIC * One row per USGS **feature** and per Open-Meteo **response**, stored as the original JSON string.
# MAGIC * Keeping raw JSON means upstream **schema changes never break ingestion** — parsing happens in Silver.
# MAGIC * Idempotent: files already present in Bronze (by `_source_file`) are skipped.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F


def has_files(path):
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False


def loaded_files(table):
    if not spark.catalog.tableExists(table):
        return []
    return [r[0] for r in spark.table(table).select("_source_file").distinct().collect()]


# COMMAND ----------

# MAGIC %md ## Bronze · USGS events

# COMMAND ----------

BRONZE_EVENTS = t("bronze_usgs_events")
if has_files(f"{LANDING}/usgs"):
    raw = (spark.read.option("multiLine", True).json(f"{LANDING}/usgs/*.json")
           .select(F.col("_metadata.file_path").alias("_source_file"),
                   F.explode("features").alias("feature")))
    new = (raw.filter(~F.col("_source_file").isin(loaded_files(BRONZE_EVENTS)))
              .select("_source_file",
                      F.to_json("feature").alias("feature_json"),
                      F.current_timestamp().alias("_ingested_at")))
    new.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE_EVENTS)
    print("bronze_usgs_events rows:", spark.table(BRONZE_EVENTS).count())
else:
    print("no USGS files in landing")

# COMMAND ----------

# MAGIC %md ## Bronze · Open-Meteo weather

# COMMAND ----------

BRONZE_WX = t("bronze_openmeteo_weather")
if has_files(f"{LANDING}/openmeteo"):
    raw = (spark.read.option("multiLine", True).json(f"{LANDING}/openmeteo/*.json")
           .select(F.col("_metadata.file_path").alias("_source_file"),
                   F.col("quake_id"),
                   F.col("ingested_at").alias("run_ts"),
                   F.to_json("response").alias("response_json")))
    new = (raw.filter(~F.col("_source_file").isin(loaded_files(BRONZE_WX)))
              .withColumn("_ingested_at", F.current_timestamp()))
    new.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE_WX)
    print("bronze_openmeteo_weather rows:", spark.table(BRONZE_WX).count())
else:
    print("no weather files in landing")
