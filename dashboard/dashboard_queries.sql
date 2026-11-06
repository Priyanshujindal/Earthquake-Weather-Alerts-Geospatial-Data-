-- =====================================================================
-- Databricks SQL dashboard datasets (Gold layer)
-- Replace  workspace.quake_weather  with <catalog>.<schema> if you changed it.
-- Create each as a Dataset in a Lakeview dashboard (Dashboards -> Create -> Data -> Create from SQL).
-- =====================================================================

-- 1. KPI tiles ---------------------------------------------------------
SELECT COUNT(*)                         AS total_events,
       ROUND(MAX(magnitude), 1)         AS max_magnitude,
       ROUND(AVG(magnitude), 2)         AS avg_magnitude,
       ROUND(AVG(depth_km), 1)          AS avg_depth_km,
       SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) AS tsunami_flags
FROM workspace.quake_weather.gold_fact_earthquake_weather;

-- 2. Events per day (line / bar) --------------------------------------
SELECT d.full_date, COUNT(*) AS events, ROUND(AVG(f.magnitude), 2) AS avg_magnitude
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_date d USING (date_key)
GROUP BY d.full_date ORDER BY d.full_date;

-- 3. Events by magnitude class (bar) ----------------------------------
SELECT c.magnitude_class_key, c.class_name, COUNT(*) AS events
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_magnitude_class c USING (magnitude_class_key)
GROUP BY c.magnitude_class_key, c.class_name ORDER BY c.magnitude_class_key;

-- 4. Top regions (table / bar) ----------------------------------------
SELECT l.region_name, COUNT(*) AS events, ROUND(AVG(f.magnitude), 2) AS avg_magnitude,
       ROUND(MAX(f.magnitude), 1) AS max_magnitude
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_location l USING (location_key)
GROUP BY l.region_name ORDER BY events DESC LIMIT 10;

-- 5. Map: epicentres sized by magnitude (point map: lat/lon) -----------
SELECT f.event_id, f.latitude, f.longitude, f.magnitude, l.place_text,
       f.temperature_c, w.description AS weather
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_location l USING (location_key)
JOIN workspace.quake_weather.gold_dim_weather_condition w USING (weather_condition_key);

-- 6. Weather at epicentres by magnitude class (grouped bar) ------------
SELECT c.class_name, c.magnitude_class_key,
       ROUND(AVG(f.temperature_c), 1)   AS avg_temp_c,
       ROUND(AVG(f.wind_speed_kmh), 1)  AS avg_wind_kmh,
       ROUND(AVG(f.relative_humidity_pct), 1) AS avg_humidity_pct
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_magnitude_class c USING (magnitude_class_key)
WHERE f.temperature_c IS NOT NULL
GROUP BY c.class_name, c.magnitude_class_key ORDER BY c.magnitude_class_key;

-- 7. Weather conditions at epicentres (pie / bar) ----------------------
SELECT w.category, COUNT(*) AS events
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_weather_condition w USING (weather_condition_key)
GROUP BY w.category ORDER BY events DESC;

-- 8. Time-of-day pattern (bar) -----------------------------------------
SELECT t.day_part, COUNT(*) AS events
FROM workspace.quake_weather.gold_fact_earthquake_weather f
JOIN workspace.quake_weather.gold_dim_time t USING (time_key)
GROUP BY t.day_part ORDER BY MIN(t.hour);

-- 9. Pipeline health (data quality results, latest run) ----------------
SELECT check_name, severity, violations, status, run_at
FROM workspace.quake_weather.dq_results
WHERE run_at = (SELECT MAX(run_at) FROM workspace.quake_weather.dq_results)
ORDER BY status DESC, check_name;
