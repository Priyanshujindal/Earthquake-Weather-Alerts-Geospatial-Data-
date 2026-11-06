# Architecture

```mermaid
flowchart LR
    subgraph Sources
        U[USGS Earthquake<br/>GeoJSON feed]
        O[Open-Meteo<br/>Forecast API]
    end
    subgraph "Ingestion (Python)"
        I[ingest.py<br/>retry + backoff + schema guard]
    end
    subgraph "Databricks Lakehouse (Delta Lake)"
        L[(Landing Volume<br/>raw JSON)]
        B[Bronze<br/>raw JSON strings<br/>append-only]
        S[Silver<br/>typed, validated,<br/>deduplicated, 3NF]
        G[Gold<br/>star schema]
        Q[dq_results +<br/>silver_rejected_records]
    end
    D[Databricks SQL<br/>Dashboard]
    J{{Databricks Workflow<br/>hourly schedule}}

    U --> I
    O --> I
    I --> L --> B --> S --> G --> D
    S -.rejects.-> Q
    G -.checks.-> Q
    J -.orchestrates.-> I
    J -.orchestrates.-> B
    J -.orchestrates.-> S
    J -.orchestrates.-> G
```

## Layers

| Layer | Tables | Purpose |
|---|---|---|
| Landing | Volume `landing/usgs`, `landing/openmeteo` | Immutable raw API responses (replayable) |
| Bronze | `bronze_usgs_events`, `bronze_openmeteo_weather` | Raw JSON string + lineage (`_source_file`, `_ingested_at`). Tolerates schema drift |
| Silver | `silver_earthquake_event`, `silver_weather_observation`, `silver_place`, `silver_region`, `silver_network`, `silver_weather_code`, `silver_rejected_records` | Parsed, typed, validated, deduplicated, normalized (3NF) |
| Gold | `gold_fact_earthquake_weather` + 6 `gold_dim_*` | Star schema for BI |
| Ops | `dq_results` | Data-quality history |

## Local mirror
The same logic runs locally on SQLite (`src/`, `sql/`) so the pipeline can be tested without a cluster (`pytest`).
Databricks notebooks (`databricks/`) are the production implementation.

## Design decisions
* **Why Bronze stores JSON strings:** USGS/Open-Meteo can add fields at any time; storing raw JSON means ingestion never breaks. Silver parses with an explicit schema and ignores unknown fields.
* **Why two weather timestamps are possible per event:** weather is fetched on every run, so a repeating event accumulates observations; the star schema keeps the latest one per event.
* **Secrets:** both APIs are keyless. `.env` / Databricks secret scopes are the pattern if a key is added (see `.env.example`).
