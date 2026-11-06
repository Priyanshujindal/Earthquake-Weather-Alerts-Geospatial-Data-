-- Populate the star schema from the normalized (3NF) tables.
-- Run AFTER create_star_schema.sql.

-- ---- dim_date: continuous calendar from first to last event date ----
INSERT INTO dim_date
WITH RECURSIVE
bounds AS (
    SELECT date(MIN(event_time_utc)) AS d0, date(MAX(event_time_utc)) AS d1
    FROM earthquake_event
),
cal(d) AS (
    SELECT d0 FROM bounds WHERE d0 IS NOT NULL
    UNION ALL
    SELECT date(cal.d, '+1 day') FROM cal, bounds WHERE cal.d < bounds.d1
),
months(m, name) AS (VALUES (1,'January'),(2,'February'),(3,'March'),(4,'April'),(5,'May'),
    (6,'June'),(7,'July'),(8,'August'),(9,'September'),(10,'October'),(11,'November'),(12,'December')),
days(w, name) AS (VALUES (0,'Sunday'),(1,'Monday'),(2,'Tuesday'),(3,'Wednesday'),
    (4,'Thursday'),(5,'Friday'),(6,'Saturday'))
SELECT CAST(strftime('%Y%m%d', d) AS INTEGER),
       d,
       CAST(strftime('%Y', d) AS INTEGER),
       (CAST(strftime('%m', d) AS INTEGER) - 1) / 3 + 1,
       CAST(strftime('%m', d) AS INTEGER),
       months.name,
       CAST(strftime('%d', d) AS INTEGER),
       CAST(strftime('%w', d) AS INTEGER),
       days.name,
       CASE WHEN CAST(strftime('%w', d) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END
FROM cal
JOIN months ON months.m = CAST(strftime('%m', d) AS INTEGER)
JOIN days   ON days.w   = CAST(strftime('%w', d) AS INTEGER);

-- ---- dim_time: 24 hours ----
INSERT INTO dim_time
WITH RECURSIVE h(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM h WHERE x < 23)
SELECT x, x,
       CASE WHEN x < 6 THEN 'Night' WHEN x < 12 THEN 'Morning'
            WHEN x < 18 THEN 'Afternoon' ELSE 'Evening' END
FROM h;

-- ---- dim_location ----
INSERT INTO dim_location (location_key, place_text, region_name)
SELECT p.place_id, p.place_text, r.region_name
FROM place p JOIN region r ON r.region_id = p.region_id;

-- ---- dim_network ----
INSERT INTO dim_network (network_code, network_name)
SELECT network_code, network_name FROM network ORDER BY network_code;

-- ---- dim_magnitude_class (static, Richter-style bands) ----
INSERT INTO dim_magnitude_class VALUES
 (1,'Micro',    -10, 2, 'Not felt'),
 (2,'Minor',      2, 3, 'Rarely felt'),
 (3,'Light',      3, 4, 'Often felt, rarely damaging'),
 (4,'Moderate',   4, 5, 'Noticeable shaking, minor damage'),
 (5,'Strong',     5, 6, 'Damage to poorly built structures'),
 (6,'Major',      6, 7, 'Destructive in populated areas'),
 (7,'Great',      7, 100,'Serious damage over large areas');

-- ---- dim_weather_condition (+ unknown member) ----
INSERT INTO dim_weather_condition VALUES (-1, 'Unknown / not observed', 'Unknown');
INSERT INTO dim_weather_condition
SELECT weather_code, description, category FROM weather_code;

-- ---- fact ----
INSERT INTO fact_earthquake_weather
SELECT e.event_id,
       CAST(strftime('%Y%m%d', e.event_time_utc) AS INTEGER),
       CAST(strftime('%H', e.event_time_utc) AS INTEGER),
       e.place_id,
       n.network_key,
       mc.magnitude_class_key,
       COALESCE(w.weather_code, -1),
       e.magnitude, e.depth_km, e.latitude, e.longitude,
       e.sig, e.tsunami, e.felt,
       w.temperature_c, w.relative_humidity_pct, w.precipitation_mm, w.wind_speed_kmh,
       w.observation_time_utc
FROM earthquake_event e
JOIN dim_network n           ON n.network_code = e.network_code
JOIN dim_magnitude_class mc  ON e.magnitude >= mc.min_mag AND e.magnitude < mc.max_mag
LEFT JOIN weather_observation w
       ON w.event_id = e.event_id
      AND w.observation_time_utc = (SELECT MAX(w2.observation_time_utc)
                                    FROM weather_observation w2
                                    WHERE w2.event_id = e.event_id);
