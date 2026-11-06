# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: star schema (`docs/star_schema.md`)
# MAGIC Grain of `gold_fact_earthquake_weather`: **one row per earthquake**, joined to its **latest** weather observation.
# MAGIC Gold tables are fully rebuilt from Silver each run (deterministic hash keys keep keys stable between runs).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F, Window


def save_delta(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(name))


ev = spark.table(t("silver_earthquake_event"))
bounds = ev.agg(F.min(F.to_date("event_time_utc")).alias("d0"), F.max(F.to_date("event_time_utc")).alias("d1")).collect()[0]
assert bounds["d0"] is not None, "silver_earthquake_event is empty - run notebooks 01-03 first"

# COMMAND ----------

# MAGIC %md ## Dimensions

# COMMAND ----------

dim_date = (spark.sql(f"SELECT explode(sequence(to_date('{bounds['d0']}'), to_date('{bounds['d1']}'), interval 1 day)) AS full_date")
    .select(F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date",
            F.year("full_date").alias("year"), F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"), F.date_format("full_date", "MMMM").alias("month_name"),
            F.dayofmonth("full_date").alias("day_of_month"),
            (F.dayofweek("full_date") - 1).alias("day_of_week"),            # 0=Sunday .. 6=Saturday
            F.date_format("full_date", "EEEE").alias("day_name"),
            F.dayofweek("full_date").isin(1, 7).cast("int").alias("is_weekend")))
save_delta(dim_date, "gold_dim_date")

dim_time = (spark.range(24).select(F.col("id").cast("int").alias("time_key"), F.col("id").cast("int").alias("hour"))
    .withColumn("day_part", F.when(F.col("hour") < 6, "Night").when(F.col("hour") < 12, "Morning")
                .when(F.col("hour") < 18, "Afternoon").otherwise("Evening")))
save_delta(dim_time, "gold_dim_time")

dim_location = (spark.table(t("silver_place")).join(spark.table(t("silver_region")), "region_id")
                .select(F.col("place_id").alias("location_key"), "place_text", "region_name"))
save_delta(dim_location, "gold_dim_location")

dim_network = (spark.table(t("silver_network"))
    .withColumn("network_key", F.row_number().over(Window.orderBy("network_code")))
    .select("network_key", "network_code", "network_name"))
save_delta(dim_network, "gold_dim_network")

MAG_CLASSES = [(1, "Micro", -10.0, 2.0, "Not felt"), (2, "Minor", 2.0, 3.0, "Rarely felt"),
               (3, "Light", 3.0, 4.0, "Often felt, rarely damaging"),
               (4, "Moderate", 4.0, 5.0, "Noticeable shaking, minor damage"),
               (5, "Strong", 5.0, 6.0, "Damage to poorly built structures"),
               (6, "Major", 6.0, 7.0, "Destructive in populated areas"),
               (7, "Great", 7.0, 100.0, "Serious damage over large areas")]
dim_mag = spark.createDataFrame(MAG_CLASSES, "magnitude_class_key INT, class_name STRING, min_mag DOUBLE, max_mag DOUBLE, typical_effect STRING")
save_delta(dim_mag, "gold_dim_magnitude_class")

unknown = spark.createDataFrame([(-1, "Unknown / not observed", "Unknown")], "weather_condition_key INT, description STRING, category STRING")
dim_wc = (spark.table(t("silver_weather_code")).select(F.col("weather_code").alias("weather_condition_key"), "description", "category")
          .unionByName(unknown))
save_delta(dim_wc, "gold_dim_weather_condition")

# COMMAND ----------

# MAGIC %md ## Fact

# COMMAND ----------

wx = spark.table(t("silver_weather_observation"))
latest_wx = (wx.withColumn("_rn", F.row_number().over(Window.partitionBy("event_id").orderBy(F.col("observation_time_utc").desc())))
             .filter("_rn = 1").drop("_rn"))

fact = (ev.alias("e")
    .join(spark.table(t("gold_dim_network")).alias("n"), F.col("e.network_code") == F.col("n.network_code"))
    .join(dim_mag.alias("m"), (F.col("e.magnitude") >= F.col("m.min_mag")) & (F.col("e.magnitude") < F.col("m.max_mag")))
    .join(latest_wx.alias("w"), F.col("e.event_id") == F.col("w.event_id"), "left")
    .select(F.col("e.event_id").alias("event_id"),
            F.date_format("e.event_time_utc", "yyyyMMdd").cast("int").alias("date_key"),
            F.hour("e.event_time_utc").alias("time_key"),
            F.col("e.place_id").alias("location_key"),
            F.col("n.network_key"),
            F.col("m.magnitude_class_key"),
            F.coalesce(F.col("w.weather_code"), F.lit(-1)).alias("weather_condition_key"),
            F.col("e.magnitude"), F.col("e.depth_km"), F.col("e.latitude"), F.col("e.longitude"),
            F.col("e.sig"), F.col("e.tsunami"), F.col("e.felt"),
            F.col("w.temperature_c"), F.col("w.relative_humidity_pct"),
            F.col("w.precipitation_mm"), F.col("w.wind_speed_kmh"),
            F.col("w.observation_time_utc").alias("weather_observed_at_utc")))
save_delta(fact, "gold_fact_earthquake_weather")

print("gold_fact_earthquake_weather rows:", spark.table(t("gold_fact_earthquake_weather")).count())
display(spark.table(t("gold_fact_earthquake_weather")).limit(10))
