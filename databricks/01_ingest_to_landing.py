# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest: USGS + Open-Meteo → landing Volume
# MAGIC Same logic as `src/ingest.py` (retry + exponential backoff + Retry-After). Raw JSON is saved **unmodified**.
# MAGIC
# MAGIC > **Free Edition note:** outbound internet access may be restricted. If the API calls are blocked, upload the files in
# MAGIC > `tests/sample_data/usgs` and `tests/sample_data/openmeteo` into the `landing` Volume (Catalog → Volumes → landing → Upload)
# MAGIC > and skip this notebook.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json, os, random, time
from datetime import datetime, timezone
import requests

USGS_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"
OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
RETRYABLE = {429, 500, 502, 503, 504}
HEADERS = {"User-Agent": "quake-weather-platform/1.0 (course project)"}


def get_json(url, params=None, max_retries=5, base_delay=1.0, timeout=30):
    for attempt in range(1, max_retries + 1):
        delay = base_delay * 2 ** (attempt - 1)
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code in RETRYABLE:
                ra = r.headers.get("Retry-After", "")
                if ra.isdigit():
                    delay = float(ra)
                raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            err = exc
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE:
                raise
            err = exc
        if attempt == max_retries:
            raise err
        time.sleep(delay + random.uniform(0, 0.5))


def save(obj, subdir, name):
    folder = f"{LANDING}/{subdir}"
    os.makedirs(folder, exist_ok=True)
    with open(f"{folder}/{name}", "w") as f:
        json.dump(obj, f)
    print("saved", f"{folder}/{name}")


# COMMAND ----------

run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
quakes = get_json(USGS_FEED.format(feed=FEED))
assert "features" in quakes, "Unexpected USGS payload"
save(quakes, "usgs", f"usgs_{FEED}_{run_ts}.json")

ok = failed = 0
for feat in quakes["features"][:MAX_QUAKES]:
    try:
        lon, lat = feat["geometry"]["coordinates"][:2]
        wx = get_json(OPEN_METEO, params={
            "latitude": lat, "longitude": lon, "timezone": "UTC",
            "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code"})
        save({"quake_id": feat["id"], "ingested_at": run_ts, "response": wx},
             "openmeteo", f"weather_{feat['id']}_{run_ts}.json")
        ok += 1
    except Exception as exc:
        print("weather failed for", feat.get("id"), exc)
        failed += 1
    time.sleep(0.3)

print(f"events={len(quakes['features'])} weather_ok={ok} weather_failed={failed}")
