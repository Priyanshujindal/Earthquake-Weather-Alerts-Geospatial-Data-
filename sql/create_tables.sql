-- =====================================================================
-- Normalized (3NF) schema  -  SQLite dialect (local build)
-- Idempotent: safe to run repeatedly.
-- =====================================================================
PRAGMA foreign_keys = ON;

-- ---------- Lookup / reference tables ----------
CREATE TABLE IF NOT EXISTS network (
    network_code  TEXT PRIMARY KEY,            -- USGS "net" code, e.g. 'ci'
    network_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS magnitude_type (
    mag_type      TEXT PRIMARY KEY             -- ml, mb, mww, md ...
);

CREATE TABLE IF NOT EXISTS event_type (
    event_type    TEXT PRIMARY KEY             -- earthquake, quarry blast ...
);

CREATE TABLE IF NOT EXISTS region (
    region_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    region_name   TEXT NOT NULL UNIQUE         -- 'CA', 'Japan', 'Mid-Atlantic Ridge'
);

CREATE TABLE IF NOT EXISTS place (
    place_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    place_text    TEXT NOT NULL UNIQUE,        -- '9 km SW of Ridgecrest, CA'
    region_id     INTEGER NOT NULL REFERENCES region(region_id)
);

CREATE TABLE IF NOT EXISTS weather_code (      -- WMO weather interpretation codes
    weather_code  INTEGER PRIMARY KEY,
    description   TEXT NOT NULL,
    category      TEXT NOT NULL
);

-- ---------- Core entities ----------
CREATE TABLE IF NOT EXISTS earthquake_event (
    event_id          TEXT PRIMARY KEY,        -- USGS stable id (dedup key)
    network_code      TEXT NOT NULL REFERENCES network(network_code),
    mag_type          TEXT REFERENCES magnitude_type(mag_type),
    event_type        TEXT NOT NULL REFERENCES event_type(event_type),
    place_id          INTEGER NOT NULL REFERENCES place(place_id),
    magnitude         REAL NOT NULL,
    latitude          REAL NOT NULL,
    longitude         REAL NOT NULL,
    depth_km          REAL,
    event_time_utc    TEXT NOT NULL,           -- 'YYYY-MM-DD HH:MM:SS'
    updated_time_utc  TEXT NOT NULL,           -- USGS revision timestamp (dedup version)
    sig               INTEGER,
    tsunami           INTEGER,
    felt              INTEGER,
    cdi               REAL,
    mmi               REAL,
    alert             TEXT,
    status            TEXT,
    url               TEXT,
    source_file       TEXT,
    loaded_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS weather_observation (
    observation_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id               TEXT NOT NULL REFERENCES earthquake_event(event_id),
    observation_time_utc   TEXT NOT NULL,
    temperature_c          REAL,
    relative_humidity_pct  REAL,
    precipitation_mm       REAL,
    wind_speed_kmh         REAL,
    weather_code           INTEGER REFERENCES weather_code(weather_code),
    grid_latitude          REAL,
    grid_longitude         REAL,
    elevation_m            REAL,
    ingested_at_utc        TEXT NOT NULL,
    source_file            TEXT,
    UNIQUE (event_id, observation_time_utc)    -- dedup key
);

-- ---------- Operational tables (lineage + data quality) ----------
CREATE TABLE IF NOT EXISTS ingestion_log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file     TEXT NOT NULL,
    sha256          TEXT NOT NULL UNIQUE,      -- makes file loads idempotent
    source_type     TEXT NOT NULL,             -- 'usgs' | 'openmeteo'
    rows_read       INTEGER,
    inserted        INTEGER,
    updated         INTEGER,
    skipped_stale   INTEGER,
    rejected        INTEGER,
    loaded_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rejected_record (
    reject_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file  TEXT,
    record_key   TEXT,
    reason       TEXT NOT NULL,
    raw_json     TEXT,
    rejected_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_event_time  ON earthquake_event(event_time_utc);
CREATE INDEX IF NOT EXISTS ix_event_place ON earthquake_event(place_id);
CREATE INDEX IF NOT EXISTS ix_wx_event    ON weather_observation(event_id);

-- ---------- Seed reference data ----------
INSERT OR IGNORE INTO network (network_code, network_name) VALUES
 ('us','USGS National Earthquake Information Center'),
 ('ci','Southern California Seismic Network'),
 ('nc','Northern California Seismic Network'),
 ('nn','Nevada Seismological Laboratory'),
 ('hv','Hawaiian Volcano Observatory'),
 ('ak','Alaska Earthquake Center'),
 ('uw','Pacific Northwest Seismic Network'),
 ('pr','Puerto Rico Seismic Network'),
 ('mb','Montana Bureau of Mines and Geology'),
 ('ok','Oklahoma Geological Survey'),
 ('tx','Texas Seismological Network'),
 ('unknown','Unknown network');

INSERT OR IGNORE INTO weather_code (weather_code, description, category) VALUES
 (0,'Clear sky','Clear'),(1,'Mainly clear','Clear'),(2,'Partly cloudy','Cloudy'),
 (3,'Overcast','Cloudy'),(45,'Fog','Fog'),(48,'Depositing rime fog','Fog'),
 (51,'Light drizzle','Drizzle'),(53,'Moderate drizzle','Drizzle'),(55,'Dense drizzle','Drizzle'),
 (56,'Light freezing drizzle','Drizzle'),(57,'Dense freezing drizzle','Drizzle'),
 (61,'Slight rain','Rain'),(63,'Moderate rain','Rain'),(65,'Heavy rain','Rain'),
 (66,'Light freezing rain','Rain'),(67,'Heavy freezing rain','Rain'),
 (71,'Slight snow','Snow'),(73,'Moderate snow','Snow'),(75,'Heavy snow','Snow'),
 (77,'Snow grains','Snow'),(80,'Slight rain showers','Rain'),(81,'Moderate rain showers','Rain'),
 (82,'Violent rain showers','Rain'),(85,'Slight snow showers','Snow'),(86,'Heavy snow showers','Snow'),
 (95,'Thunderstorm','Thunderstorm'),(96,'Thunderstorm with slight hail','Thunderstorm'),
 (99,'Thunderstorm with heavy hail','Thunderstorm');
