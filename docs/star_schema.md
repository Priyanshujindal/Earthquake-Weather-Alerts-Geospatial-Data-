# Star schema

**Grain:** one row per earthquake event, enriched with the *most recent* weather observation at its epicentre.
Built by `sql/transform_star.sql` (local) and `databricks/04_gold.py` (Databricks).

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_EARTHQUAKE_WEATHER : date_key
    DIM_TIME ||--o{ FACT_EARTHQUAKE_WEATHER : time_key
    DIM_LOCATION ||--o{ FACT_EARTHQUAKE_WEATHER : location_key
    DIM_NETWORK ||--o{ FACT_EARTHQUAKE_WEATHER : network_key
    DIM_MAGNITUDE_CLASS ||--o{ FACT_EARTHQUAKE_WEATHER : magnitude_class_key
    DIM_WEATHER_CONDITION ||--o{ FACT_EARTHQUAKE_WEATHER : weather_condition_key

    FACT_EARTHQUAKE_WEATHER {
        text event_id PK "degenerate dimension"
        int date_key FK
        int time_key FK
        int location_key FK
        int network_key FK
        int magnitude_class_key FK
        int weather_condition_key FK
        real magnitude "measure"
        real depth_km "measure"
        real latitude
        real longitude
        int sig "measure"
        int tsunami "measure"
        int felt "measure"
        real temperature_c "measure"
        real relative_humidity_pct "measure"
        real precipitation_mm "measure"
        real wind_speed_kmh "measure"
        text weather_observed_at_utc
    }
    DIM_DATE {
        int date_key PK "yyyymmdd"
        date full_date
        int year
        int quarter
        int month
        text month_name
        int day_of_month
        int day_of_week
        text day_name
        int is_weekend
    }
    DIM_TIME {
        int time_key PK "hour 0-23"
        int hour
        text day_part
    }
    DIM_LOCATION {
        int location_key PK
        text place_text
        text region_name
    }
    DIM_NETWORK {
        int network_key PK
        text network_code
        text network_name
    }
    DIM_MAGNITUDE_CLASS {
        int magnitude_class_key PK
        text class_name
        real min_mag
        real max_mag
        text typical_effect
    }
    DIM_WEATHER_CONDITION {
        int weather_condition_key PK "WMO code, -1 = unknown"
        text description
        text category
    }
```

## Modelling notes
* Events without a weather reading point to the **Unknown (-1)** weather member, so inner joins never drop earthquakes.
* `dim_magnitude_class` bands are half-open `[min, max)`.
* `dim_date` is generated as a *continuous* calendar between the first and last event date (no gaps).
* Dimension keys in Databricks are deterministic (`xxhash64` / ordered `row_number`) so full rebuilds are repeatable.

## Example business questions
| Question | Tables |
|---|---|
| Which regions have the most / strongest quakes? | fact + dim_location |
| Do large quakes cluster at a time of day? | fact + dim_time |
| What was the weather at the epicentre during major quakes? | fact + dim_weather_condition + dim_magnitude_class |
| Which seismic network reports the most events? | fact + dim_network |
