"""
ingest.py - Pull earthquakes from USGS, then current weather from Open-Meteo for
each earthquake location. Raw JSON is written UNMODIFIED to the landing zone.

Usage:
    python src/ingest.py --feed all_day --max-quakes 25
    python src/ingest.py --feed 2.5_week --landing landing_zone
"""
import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:  # optional: load .env if python-dotenv is installed
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:  # pragma: no cover
    pass

USGS_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"
OPEN_METEO = os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
DEFAULT_LANDING = Path(__file__).resolve().parent.parent / "landing_zone"
RETRYABLE = {429, 500, 502, 503, 504}
HEADERS = {"User-Agent": "quake-weather-platform/1.0 (course project)"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")


def get_json(url, params=None, max_retries=5, base_delay=1.0, timeout=30):
    """GET with exponential backoff + jitter; honors Retry-After on HTTP 429."""
    for attempt in range(1, max_retries + 1):
        delay = base_delay * 2 ** (attempt - 1)
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            if resp.status_code in RETRYABLE:
                retry_after = resp.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = float(retry_after)
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()  # other 4xx are raised and NOT retried
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as exc:
            err = exc
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE:
                raise
            err = exc
        if attempt == max_retries:
            log.error("Giving up on %s after %d attempts: %s", url, attempt, err)
            raise err
        delay += random.uniform(0, 0.5)
        log.warning("Attempt %d failed (%s). Retrying in %.1fs", attempt, err, delay)
        time.sleep(delay)


def save(obj, landing, subdir, name):
    folder = Path(landing) / subdir
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(json.dumps(obj, indent=2))
    log.info("Saved %s", path)
    return path


def run(feed="all_day", max_quakes=25, pause=0.3, landing=DEFAULT_LANDING):
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    quakes = get_json(USGS_FEED.format(feed=feed))
    if "features" not in quakes:  # schema guard: fail loudly if USGS changes shape
        raise ValueError("Unexpected USGS payload: no 'features' key")
    save(quakes, landing, "usgs", f"usgs_{feed}_{run_ts}.json")
    features = quakes["features"]
    log.info("USGS returned %d events", len(features))

    ok = failed = 0
    for feat in features[:max_quakes]:
        quake_id = feat.get("id")
        try:
            lon, lat = feat["geometry"]["coordinates"][:2]
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,precipitation,"
                           "wind_speed_10m,weather_code",
                "timezone": "UTC",
            }
            if os.getenv("OPEN_METEO_API_KEY"):
                params["apikey"] = os.environ["OPEN_METEO_API_KEY"]
            weather = get_json(OPEN_METEO, params=params)
            save({"quake_id": quake_id, "ingested_at": run_ts, "response": weather},
                 landing, "openmeteo", f"weather_{quake_id}_{run_ts}.json")
            ok += 1
        except Exception as exc:  # one bad event must not kill the whole run
            log.error("Weather failed for %s: %s", quake_id, exc)
            failed += 1
        time.sleep(pause)

    log.info("Done. weather ok=%d failed=%d", ok, failed)
    return {"events": len(features), "weather_ok": ok, "weather_failed": failed}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--feed", default="all_day",
                    help="USGS feed: all_hour, all_day, 2.5_day, 2.5_week, 4.5_month ...")
    ap.add_argument("--max-quakes", type=int, default=25,
                    help="Cap weather lookups to stay well under rate limits")
    ap.add_argument("--pause", type=float, default=0.3, help="Seconds between weather calls")
    ap.add_argument("--landing", default=str(DEFAULT_LANDING), help="Landing zone folder")
    a = ap.parse_args(argv)
    return run(a.feed, a.max_quakes, a.pause, a.landing)


if __name__ == "__main__":
    main()
