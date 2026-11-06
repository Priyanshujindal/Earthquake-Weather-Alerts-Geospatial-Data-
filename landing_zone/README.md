# landing_zone

Raw, **unmodified** API responses written by `src/ingest.py`:

```
landing_zone/
├── usgs/        usgs_<feed>_<UTC run timestamp>.json       (GeoJSON FeatureCollection)
└── openmeteo/   weather_<quake_id>_<UTC run timestamp>.json (Open-Meteo response + quake_id)
```

Run `python src/ingest.py --max-quakes 5` and **commit the files it creates** – they are the
"sample ingested JSON" for the Week 4 submission. (Synthetic files for offline tests live in `tests/sample_data/`.)
