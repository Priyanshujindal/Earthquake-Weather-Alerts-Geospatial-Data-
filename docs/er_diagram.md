# Normalized (3NF) ER diagram

Implemented in `sql/create_tables.sql` (local) and the `silver_*` tables (Databricks).

```mermaid
erDiagram
    NETWORK ||--o{ EARTHQUAKE_EVENT : reports
    MAGNITUDE_TYPE |o--o{ EARTHQUAKE_EVENT : "measured with"
    EVENT_TYPE ||--o{ EARTHQUAKE_EVENT : classifies
    REGION ||--o{ PLACE : contains
    PLACE ||--o{ EARTHQUAKE_EVENT : "occurs at"
    EARTHQUAKE_EVENT ||--o{ WEATHER_OBSERVATION : "has weather"
    WEATHER_CODE |o--o{ WEATHER_OBSERVATION : describes

    NETWORK {
        text network_code PK
        text network_name
    }
    MAGNITUDE_TYPE {
        text mag_type PK
    }
    EVENT_TYPE {
        text event_type PK
    }
    REGION {
        int region_id PK
        text region_name UK
    }
    PLACE {
        int place_id PK
        text place_text UK
        int region_id FK
    }
    WEATHER_CODE {
        int weather_code PK
        text description
        text category
    }
    EARTHQUAKE_EVENT {
        text event_id PK "USGS stable id"
        text network_code FK
        text mag_type FK
        text event_type FK
        int place_id FK
        real magnitude
        real latitude
        real longitude
        real depth_km
        text event_time_utc
        text updated_time_utc "dedup version"
        int sig
        int tsunami
        int felt
        text status
        text url
    }
    WEATHER_OBSERVATION {
        int observation_id PK
        text event_id FK
        text observation_time_utc "UK with event_id"
        real temperature_c
        real relative_humidity_pct
        real precipitation_mm
        real wind_speed_kmh
        int weather_code FK
        real elevation_m
        text ingested_at_utc
    }
```

Operational tables (not part of the business model): `ingestion_log` (file lineage + idempotency), `rejected_record` (quarantine).

## Why this is 3NF
* **1NF** – every column is atomic; the GeoJSON `coordinates` array is split into `longitude`, `latitude`, `depth_km`; the Open-Meteo `current` object is flattened.
* **2NF** – every table has a single-column key (except `weather_observation`'s natural key, whose non-key columns depend on the whole `(event_id, observation_time)` pair).
* **3NF** – transitive dependencies removed: `event → place → region` (place text determines region), `event → network → network_name`, `weather_code → description/category`.
