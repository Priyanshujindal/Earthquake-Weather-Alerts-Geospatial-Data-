-- =====================================================================
-- Star schema (analytical layer)  -  SQLite dialect (local build)
-- Full refresh each run: drop + recreate (cheap at this scale).
-- Grain of fact_earthquake_weather: ONE ROW PER EARTHQUAKE EVENT,
-- joined to its most recent weather observation.
-- =====================================================================
DROP TABLE IF EXISTS fact_earthquake_weather;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_network;
DROP TABLE IF EXISTS dim_magnitude_class;
DROP TABLE IF EXISTS dim_weather_condition;

CREATE TABLE dim_date (
    date_key      INTEGER PRIMARY KEY,         -- yyyymmdd
    full_date     TEXT NOT NULL,
    year          INTEGER NOT NULL,
    quarter       INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT NOT NULL,
    day_of_month  INTEGER NOT NULL,
    day_of_week   INTEGER NOT NULL,            -- 0=Sunday .. 6=Saturday
    day_name      TEXT NOT NULL,
    is_weekend    INTEGER NOT NULL
);

CREATE TABLE dim_time (
    time_key   INTEGER PRIMARY KEY,            -- hour of day 0-23
    hour       INTEGER NOT NULL,
    day_part   TEXT NOT NULL
);

CREATE TABLE dim_location (
    location_key  INTEGER PRIMARY KEY,         -- = place.place_id
    place_text    TEXT NOT NULL,
    region_name   TEXT NOT NULL
);

CREATE TABLE dim_network (
    network_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    network_code  TEXT NOT NULL UNIQUE,
    network_name  TEXT NOT NULL
);

CREATE TABLE dim_magnitude_class (
    magnitude_class_key  INTEGER PRIMARY KEY,
    class_name           TEXT NOT NULL,
    min_mag              REAL NOT NULL,        -- inclusive
    max_mag              REAL NOT NULL,        -- exclusive
    typical_effect       TEXT NOT NULL
);

CREATE TABLE dim_weather_condition (
    weather_condition_key  INTEGER PRIMARY KEY, -- WMO code; -1 = unknown
    description            TEXT NOT NULL,
    category               TEXT NOT NULL
);

CREATE TABLE fact_earthquake_weather (
    event_id                TEXT PRIMARY KEY,   -- degenerate dimension
    date_key                INTEGER NOT NULL REFERENCES dim_date(date_key),
    time_key                INTEGER NOT NULL REFERENCES dim_time(time_key),
    location_key            INTEGER NOT NULL REFERENCES dim_location(location_key),
    network_key             INTEGER NOT NULL REFERENCES dim_network(network_key),
    magnitude_class_key     INTEGER NOT NULL REFERENCES dim_magnitude_class(magnitude_class_key),
    weather_condition_key   INTEGER NOT NULL REFERENCES dim_weather_condition(weather_condition_key),
    -- measures
    magnitude               REAL NOT NULL,
    depth_km                REAL,
    latitude                REAL NOT NULL,
    longitude               REAL NOT NULL,
    sig                     INTEGER,
    tsunami                 INTEGER,
    felt                    INTEGER,
    temperature_c           REAL,
    relative_humidity_pct   REAL,
    precipitation_mm        REAL,
    wind_speed_kmh          REAL,
    weather_observed_at_utc TEXT
);

CREATE INDEX ix_fact_date ON fact_earthquake_weather(date_key);
CREATE INDEX ix_fact_loc  ON fact_earthquake_weather(location_key);
