# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: parse, validate, deduplicate, normalize (3NF)
# MAGIC Tables produced (mirrors the ER diagram in `docs/er_diagram.md`):
# MAGIC `silver_region`, `silver_place`, `silver_network`, `silver_weather_code`, `silver_earthquake_event`,
# MAGIC `silver_weather_observation`, `silver_rejected_records`.
# MAGIC
# MAGIC **Dedup:** Delta `MERGE` on `event_id`, updating only when incoming `updated_time_utc` is newer.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

EVENT_SCHEMA = (
    "id STRING, "
    "properties STRUCT<mag: DOUBLE, place: STRING, time: BIGINT, updated: BIGINT, felt: INT, cdi: DOUBLE, "
    "mmi: DOUBLE, alert: STRING, status: STRING, tsunami: INT, sig: INT, net: STRING, magType: STRING, "
    "type: STRING, url: STRING>, "
    "geometry STRUCT<coordinates: ARRAY<DOUBLE>>"
)  # fields not listed are ignored -> tolerant of upstream schema evolution


def save_delta(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(name))


def merge_upsert(df, name, key_cond, version_col):
    target = t(name)
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return
    (DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), key_cond)
        .whenMatchedUpdateAll(condition=f"s.{version_col} > t.{version_col}")
        .whenNotMatchedInsertAll()
        .execute())


# COMMAND ----------

# MAGIC %md ## 1 · Parse USGS events

# COMMAND ----------

p = F.col("f.properties")
place = F.coalesce(p["place"], F.lit("Unknown"))
region_tail = F.regexp_extract(place, r",\s*([^,]+)$", 1)
region_of = F.regexp_extract(place, r" of (.+)$", 1)

parsed = (spark.table(t("bronze_usgs_events"))
    .select(F.from_json("feature_json", EVENT_SCHEMA).alias("f"), "_source_file", "_ingested_at")
    .select(
        F.col("f.id").alias("event_id"),
        F.coalesce(p["net"], F.lit("unknown")).alias("network_code"),
        p["magType"].alias("mag_type"),
        F.coalesce(p["type"], F.lit("unknown")).alias("event_type"),
        place.alias("place_text"),
        F.when(region_tail != "", region_tail).when(region_of != "", region_of).otherwise(place).alias("region_name"),
        p["mag"].alias("magnitude"),
        F.col("f.geometry.coordinates")[1].alias("latitude"),
        F.col("f.geometry.coordinates")[0].alias("longitude"),
        F.col("f.geometry.coordinates")[2].alias("depth_km"),
        (p["time"] / 1000).cast("timestamp").alias("event_time_utc"),
        (p["updated"] / 1000).cast("timestamp").alias("updated_time_utc"),
        p["sig"].alias("sig"), p["tsunami"].alias("tsunami"), p["felt"].alias("felt"),
        p["cdi"].alias("cdi"), p["mmi"].alias("mmi"), p["alert"].alias("alert"),
        p["status"].alias("status"), p["url"].alias("url"),
        "_source_file", "_ingested_at"))

reason = F.concat_ws("; ",
    F.when(F.col("event_id").isNull(), F.lit("missing event id")),
    F.when(~F.coalesce(F.col("magnitude").between(-2, 10), F.lit(False)), F.lit("magnitude null/out of range")),
    F.when(~F.coalesce(F.col("latitude").between(-90, 90), F.lit(False)), F.lit("latitude invalid")),
    F.when(~F.coalesce(F.col("longitude").between(-180, 180), F.lit(False)), F.lit("longitude invalid")),
    F.when(F.col("depth_km").isNotNull() & ~F.col("depth_km").between(-5, 800), F.lit("depth out of range")),
    F.when(F.col("event_time_utc").isNull(), F.lit("missing event time")),
    F.when(F.col("updated_time_utc").isNull(), F.lit("missing updated time")))

flagged = parsed.withColumn("reject_reason", reason)
good_events = flagged.filter(F.col("reject_reason") == "").drop("reject_reason")
bad_events = flagged.filter(F.col("reject_reason") != "")

# COMMAND ----------

# MAGIC %md ## 2 · Reference tables (region → place, network, weather codes)

# COMMAND ----------

save_delta(good_events.select("region_name").distinct()
           .withColumn("region_id", F.xxhash64("region_name")), "silver_region")

save_delta(good_events.select("place_text", "region_name").distinct()
           .withColumn("place_id", F.xxhash64("place_text"))
           .withColumn("region_id", F.xxhash64("region_name"))
           .drop("region_name"), "silver_place")

NETWORKS = [("us", "USGS National Earthquake Information Center"), ("ci", "Southern California Seismic Network"),
            ("nc", "Northern California Seismic Network"), ("nn", "Nevada Seismological Laboratory"),
            ("hv", "Hawaiian Volcano Observatory"), ("ak", "Alaska Earthquake Center"),
            ("uw", "Pacific Northwest Seismic Network"), ("pr", "Puerto Rico Seismic Network"),
            ("unknown", "Unknown network")]
net_ref = spark.createDataFrame(NETWORKS, "network_code STRING, network_name STRING")
nets = (good_events.select("network_code").distinct().join(net_ref, "network_code", "left")
        .withColumn("network_name", F.coalesce("network_name", F.concat(F.lit("Unknown network ("), "network_code", F.lit(")")))))
save_delta(nets, "silver_network")

WMO = [(0, "Clear sky", "Clear"), (1, "Mainly clear", "Clear"), (2, "Partly cloudy", "Cloudy"), (3, "Overcast", "Cloudy"),
       (45, "Fog", "Fog"), (48, "Depositing rime fog", "Fog"), (51, "Light drizzle", "Drizzle"),
       (53, "Moderate drizzle", "Drizzle"), (55, "Dense drizzle", "Drizzle"), (56, "Light freezing drizzle", "Drizzle"),
       (57, "Dense freezing drizzle", "Drizzle"), (61, "Slight rain", "Rain"), (63, "Moderate rain", "Rain"),
       (65, "Heavy rain", "Rain"), (66, "Light freezing rain", "Rain"), (67, "Heavy freezing rain", "Rain"),
       (71, "Slight snow", "Snow"), (73, "Moderate snow", "Snow"), (75, "Heavy snow", "Snow"), (77, "Snow grains", "Snow"),
       (80, "Slight rain showers", "Rain"), (81, "Moderate rain showers", "Rain"), (82, "Violent rain showers", "Rain"),
       (85, "Slight snow showers", "Snow"), (86, "Heavy snow showers", "Snow"), (95, "Thunderstorm", "Thunderstorm"),
       (96, "Thunderstorm with slight hail", "Thunderstorm"), (99, "Thunderstorm with heavy hail", "Thunderstorm")]
save_delta(spark.createDataFrame(WMO, "weather_code INT, description STRING, category STRING"), "silver_weather_code")

# COMMAND ----------

# MAGIC %md ## 3 · Deduplicate + MERGE events (latest `updated` wins)

# COMMAND ----------

w = Window.partitionBy("event_id").orderBy(F.col("updated_time_utc").desc(), F.col("_ingested_at").desc())
latest_events = (good_events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
                 .withColumn("place_id", F.xxhash64("place_text"))
                 .drop("place_text", "region_name"))

merge_upsert(latest_events, "silver_earthquake_event", "t.event_id = s.event_id", "updated_time_utc")
print("silver_earthquake_event rows:", spark.table(t("silver_earthquake_event")).count())

# COMMAND ----------

# MAGIC %md ## 4 · Weather observations

# COMMAND ----------

WX_SCHEMA = ("latitude DOUBLE, longitude DOUBLE, elevation DOUBLE, "
             "current STRUCT<time: STRING, temperature_2m: DOUBLE, relative_humidity_2m: DOUBLE, "
             "precipitation: DOUBLE, wind_speed_10m: DOUBLE, weather_code: INT>")

wx = (spark.table(t("bronze_openmeteo_weather"))
      .select(F.col("quake_id").alias("event_id"),
              F.from_json("response_json", WX_SCHEMA).alias("r"),
              F.to_timestamp("run_ts", "yyyyMMdd'T'HHmmss'Z'").alias("ingested_at_utc"),
              "_source_file", "response_json")
      .select("event_id",
              F.to_timestamp("r.current.time", "yyyy-MM-dd'T'HH:mm").alias("observation_time_utc"),
              F.col("r.current.temperature_2m").alias("temperature_c"),
              F.col("r.current.relative_humidity_2m").alias("relative_humidity_pct"),
              F.col("r.current.precipitation").alias("precipitation_mm"),
              F.col("r.current.wind_speed_10m").alias("wind_speed_kmh"),
              F.col("r.current.weather_code").alias("weather_code"),
              F.col("r.latitude").alias("grid_latitude"),
              F.col("r.longitude").alias("grid_longitude"),
              F.col("r.elevation").alias("elevation_m"),
              "ingested_at_utc", "_source_file", "response_json"))

known_events = spark.table(t("silver_earthquake_event")).select("event_id").distinct()
wx_checked = (wx.join(known_events.withColumn("_known", F.lit(True)), "event_id", "left")
    .withColumn("reject_reason", F.concat_ws("; ",
        F.when(F.col("event_id").isNull(), F.lit("missing quake_id")),
        F.when(F.col("_known").isNull(), F.lit("unknown event_id")),
        F.when(F.col("observation_time_utc").isNull(), F.lit("invalid observation time")),
        F.when(~F.coalesce(F.col("temperature_c").between(-90, 60), F.lit(True)), F.lit("temperature out of range")),
        F.when(~F.coalesce(F.col("relative_humidity_pct").between(0, 100), F.lit(True)), F.lit("humidity out of range")),
        F.when(F.col("wind_speed_kmh") < 0, F.lit("negative wind")),
        F.when(F.col("precipitation_mm") < 0, F.lit("negative precipitation")))))

good_wx = wx_checked.filter(F.col("reject_reason") == "").drop("reject_reason", "_known", "response_json")
bad_wx = wx_checked.filter(F.col("reject_reason") != "")

ww = Window.partitionBy("event_id", "observation_time_utc").orderBy(F.col("ingested_at_utc").desc())
latest_wx = good_wx.withColumn("_rn", F.row_number().over(ww)).filter("_rn = 1").drop("_rn")

target_wx = t("silver_weather_observation")
if not spark.catalog.tableExists(target_wx):
    latest_wx.write.format("delta").saveAsTable(target_wx)
else:
    (DeltaTable.forName(spark, target_wx).alias("t")
        .merge(latest_wx.alias("s"), "t.event_id = s.event_id AND t.observation_time_utc = s.observation_time_utc")
        .whenMatchedUpdateAll(condition="s.ingested_at_utc > t.ingested_at_utc")
        .whenNotMatchedInsertAll().execute())
print("silver_weather_observation rows:", spark.table(target_wx).count())

# COMMAND ----------

# MAGIC %md ## 5 · Quarantine rejected records (full recompute each run)

# COMMAND ----------

rejected = (bad_events.select(F.lit("usgs_event").alias("source"), F.col("event_id").alias("record_key"),
                              "reject_reason", F.col("_source_file"))
            .unionByName(bad_wx.select(F.lit("openmeteo_weather").alias("source"),
                                       F.col("event_id").alias("record_key"), "reject_reason", "_source_file"))
            .withColumn("_rejected_at", F.current_timestamp()))
save_delta(rejected, "silver_rejected_records")
print("rejected:", rejected.count())
