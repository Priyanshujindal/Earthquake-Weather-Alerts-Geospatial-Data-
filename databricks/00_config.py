# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config & setup
# MAGIC Shared settings. Every other notebook starts with `%run ./00_config`.
# MAGIC Edit the defaults below (or override with notebook widgets).
# MAGIC
# MAGIC Layout (Unity Catalog): `<catalog>.<schema>` holds all Delta tables and a managed **Volume** `landing` for raw JSON.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "quake_weather", "Schema")
dbutils.widgets.text("feed", "all_day", "USGS feed")
dbutils.widgets.text("max_quakes", "25", "Max weather lookups")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FEED = dbutils.widgets.get("feed")
MAX_QUAKES = int(dbutils.widgets.get("max_quakes"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"


def t(name: str) -> str:
    """Fully-qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"


print(f"catalog={CATALOG} schema={SCHEMA} landing={LANDING}")
