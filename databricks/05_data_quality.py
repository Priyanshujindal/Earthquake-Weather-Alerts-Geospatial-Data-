# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data-quality gate
# MAGIC Runs checks on Silver + Gold, stores results in `dq_results`, and **fails the job** if any hard check fails.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

F_ = t("gold_fact_earthquake_weather")
E_ = t("silver_earthquake_event")

# (name, severity, violations-count SQL)
CHECKS = [
    ("event_id_unique_not_null", "error",
     f"SELECT COUNT(*) - COUNT(DISTINCT event_id) + COUNT_IF(event_id IS NULL) FROM {E_}"),
    ("magnitude_in_range", "error", f"SELECT COUNT(*) FROM {E_} WHERE magnitude NOT BETWEEN -2 AND 10"),
    ("lat_lon_in_range", "error",
     f"SELECT COUNT(*) FROM {E_} WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180"),
    ("fact_rows_equal_events", "error",
     f"SELECT ABS((SELECT COUNT(*) FROM {F_}) - (SELECT COUNT(*) FROM {E_}))"),
    ("fact_fk_date", "error",
     f"SELECT COUNT(*) FROM {F_} f LEFT ANTI JOIN {t('gold_dim_date')} d ON f.date_key = d.date_key"),
    ("fact_fk_location", "error",
     f"SELECT COUNT(*) FROM {F_} f LEFT ANTI JOIN {t('gold_dim_location')} d ON f.location_key = d.location_key"),
    ("fact_fk_network", "error",
     f"SELECT COUNT(*) FROM {F_} f LEFT ANTI JOIN {t('gold_dim_network')} d ON f.network_key = d.network_key"),
    ("fact_fk_weather", "error",
     f"SELECT COUNT(*) FROM {F_} f LEFT ANTI JOIN {t('gold_dim_weather_condition')} d ON f.weather_condition_key = d.weather_condition_key"),
    ("events_without_weather", "warn", f"SELECT COUNT(*) FROM {F_} WHERE weather_condition_key = -1"),
    ("rejected_records", "warn", f"SELECT COUNT(*) FROM {t('silver_rejected_records')}"),
]

rows = []
for name, sev, sql in CHECKS:
    bad = spark.sql(sql).collect()[0][0] or 0
    status = "PASS" if bad == 0 else ("FAIL" if sev == "error" else "WARN")
    rows.append((name, sev, int(bad), status))

res = (spark.createDataFrame(rows, "check_name STRING, severity STRING, violations INT, status STRING")
       .withColumn("run_at", F.current_timestamp()))
res.write.format("delta").mode("append").saveAsTable(t("dq_results"))
display(res)

failed = [r for r in rows if r[3] == "FAIL"]
if failed:
    raise Exception(f"Data quality FAILED: {[r[0] for r in failed]}")
print("All hard data-quality checks passed")
