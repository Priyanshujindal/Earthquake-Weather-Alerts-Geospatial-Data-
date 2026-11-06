"""
load_normalized.py - Load raw landing-zone JSON into the normalized (3NF) tables.

* Idempotent: each file is fingerprinted (sha256) in ingestion_log and skipped if seen.
* Deduplicated: events are upserted on event_id and only overwritten when the incoming
  `updated` timestamp is NEWER (see docs/deduplication_strategy.md).
* Validated: bad records go to rejected_record instead of failing the load.

Usage:
    python src/load_normalized.py --landing landing_zone --db data/quake_weather.db
"""
import argparse
import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FMT = "%Y-%m-%d %H:%M:%S"
log = logging.getLogger("load")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ----------------------------------------------------------------- helpers
def now_utc():
    return datetime.now(timezone.utc).strftime(FMT)


def iso_from_ms(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(FMT)


def iso_from_run_ts(s):
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%SZ").strftime(FMT)
    except (TypeError, ValueError):
        return now_utc()


def iso_from_openmeteo(s):
    """'2026-11-06T05:45' -> '2026-11-06 05:45:00'"""
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M").strftime(FMT)
    except (TypeError, ValueError):
        return None


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_region(place):
    """'10 km SSW of Ridgecrest, CA' -> 'CA';  'Mid-Atlantic Ridge' -> itself."""
    if not place:
        return "Unknown"
    if "," in place:
        tail = place.rsplit(",", 1)[1].strip()
        if tail:
            return tail
    if " of " in place:
        return place.split(" of ", 1)[1].strip()
    return place.strip()


def connect(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_schema(conn):
    conn.executescript((ROOT / "sql" / "create_tables.sql").read_text())
    conn.execute("PRAGMA foreign_keys = ON")


# ------------------------------------------------------------ parse/validate
def parse_feature(feat):
    p = feat.get("properties") or {}
    coords = list((feat.get("geometry") or {}).get("coordinates") or [])
    lon, lat, depth = (coords + [None, None, None])[:3]
    return {
        "event_id": feat.get("id"),
        "network_code": p.get("net") or "unknown",
        "mag_type": p.get("magType"),
        "event_type": p.get("type") or "unknown",
        "place_text": p.get("place") or "Unknown",
        "magnitude": p.get("mag"),
        "latitude": lat, "longitude": lon, "depth_km": depth,
        "event_time_utc": iso_from_ms(p.get("time")),
        "updated_time_utc": iso_from_ms(p.get("updated")),
        "sig": p.get("sig"), "tsunami": p.get("tsunami"), "felt": p.get("felt"),
        "cdi": p.get("cdi"), "mmi": p.get("mmi"), "alert": p.get("alert"),
        "status": p.get("status"), "url": p.get("url"),
    }


def validate_event(ev):
    errs = []
    if not ev["event_id"]:
        errs.append("missing event id")
    m = ev["magnitude"]
    if m is None:
        errs.append("null magnitude")
    elif not -2 <= m <= 10:
        errs.append(f"magnitude out of range: {m}")
    if ev["latitude"] is None or not -90 <= ev["latitude"] <= 90:
        errs.append(f"latitude invalid: {ev['latitude']}")
    if ev["longitude"] is None or not -180 <= ev["longitude"] <= 180:
        errs.append(f"longitude invalid: {ev['longitude']}")
    d = ev["depth_km"]
    if d is not None and not -5 <= d <= 800:
        errs.append(f"depth out of range: {d}")
    if not ev["event_time_utc"]:
        errs.append("missing event time")
    if not ev["updated_time_utc"]:
        errs.append("missing updated time")
    return errs


def validate_weather(w):
    errs = []
    if not w["event_id"]:
        errs.append("missing quake_id")
    if not w["observation_time_utc"]:
        errs.append("missing/invalid observation time")
    t = w["temperature_c"]
    if t is not None and not -90 <= t <= 60:
        errs.append(f"temperature out of range: {t}")
    h = w["relative_humidity_pct"]
    if h is not None and not 0 <= h <= 100:
        errs.append(f"humidity out of range: {h}")
    for k in ("precipitation_mm", "wind_speed_kmh"):
        if w[k] is not None and w[k] < 0:
            errs.append(f"{k} negative: {w[k]}")
    return errs


# ---------------------------------------------------------------- db helpers
def ensure_place(conn, place_text):
    region = parse_region(place_text)
    conn.execute("INSERT OR IGNORE INTO region(region_name) VALUES (?)", (region,))
    rid = conn.execute("SELECT region_id FROM region WHERE region_name=?", (region,)).fetchone()[0]
    conn.execute("INSERT OR IGNORE INTO place(place_text, region_id) VALUES (?,?)", (place_text, rid))
    return conn.execute("SELECT place_id FROM place WHERE place_text=?", (place_text,)).fetchone()[0]


def reject(conn, source_file, key, reason, raw):
    conn.execute(
        "INSERT INTO rejected_record(source_file, record_key, reason, raw_json, rejected_at) "
        "VALUES (?,?,?,?,?)",
        (source_file, key, reason, json.dumps(raw)[:20000], now_utc()),
    )


EVENT_COLS = ["event_id", "network_code", "mag_type", "event_type", "place_id", "magnitude",
              "latitude", "longitude", "depth_km", "event_time_utc", "updated_time_utc", "sig",
              "tsunami", "felt", "cdi", "mmi", "alert", "status", "url", "source_file", "loaded_at"]

UPSERT_EVENT = (
    f"INSERT INTO earthquake_event ({','.join(EVENT_COLS)}) "
    f"VALUES ({','.join('?' * len(EVENT_COLS))}) "
    "ON CONFLICT(event_id) DO UPDATE SET "
    + ",".join(f"{c}=excluded.{c}" for c in EVENT_COLS if c != "event_id")
    + " WHERE excluded.updated_time_utc > earthquake_event.updated_time_utc"
)

WX_COLS = ["event_id", "observation_time_utc", "temperature_c", "relative_humidity_pct",
           "precipitation_mm", "wind_speed_kmh", "weather_code", "grid_latitude",
           "grid_longitude", "elevation_m", "ingested_at_utc", "source_file"]

UPSERT_WX = (
    f"INSERT INTO weather_observation ({','.join(WX_COLS)}) "
    f"VALUES ({','.join('?' * len(WX_COLS))}) "
    "ON CONFLICT(event_id, observation_time_utc) DO UPDATE SET "
    + ",".join(f"{c}=excluded.{c}" for c in WX_COLS if c not in ("event_id", "observation_time_utc"))
    + " WHERE excluded.ingested_at_utc > weather_observation.ingested_at_utc"
)


# ---------------------------------------------------------------- loaders
def load_usgs_file(conn, path):
    doc = json.loads(Path(path).read_text())
    feats = doc.get("features") or []
    stats = dict(rows_read=len(feats), inserted=0, updated=0, skipped_stale=0, rejected=0)
    ts = now_utc()
    for feat in feats:
        ev = parse_feature(feat)
        errs = validate_event(ev)
        if errs:
            reject(conn, path.name, ev["event_id"], "; ".join(errs), feat)
            stats["rejected"] += 1
            continue
        existing = conn.execute("SELECT updated_time_utc FROM earthquake_event WHERE event_id=?",
                                (ev["event_id"],)).fetchone()
        conn.execute("INSERT OR IGNORE INTO network VALUES (?,?)",
                     (ev["network_code"], f"Unknown network ({ev['network_code']})"))
        conn.execute("INSERT OR IGNORE INTO event_type VALUES (?)", (ev["event_type"],))
        if ev["mag_type"]:
            conn.execute("INSERT OR IGNORE INTO magnitude_type VALUES (?)", (ev["mag_type"],))
        place_id = ensure_place(conn, ev["place_text"])
        row = dict(ev, place_id=place_id, source_file=path.name, loaded_at=ts)
        conn.execute(UPSERT_EVENT, [row[c] for c in EVENT_COLS])
        if existing is None:
            stats["inserted"] += 1
        elif ev["updated_time_utc"] > existing[0]:
            stats["updated"] += 1
        else:
            stats["skipped_stale"] += 1
    return stats


def load_weather_file(conn, path):
    doc = json.loads(Path(path).read_text())
    cur = (doc.get("response") or {}).get("current") or {}
    resp = doc.get("response") or {}
    w = {
        "event_id": doc.get("quake_id"),
        "observation_time_utc": iso_from_openmeteo(cur.get("time")),
        "temperature_c": cur.get("temperature_2m"),
        "relative_humidity_pct": cur.get("relative_humidity_2m"),
        "precipitation_mm": cur.get("precipitation"),
        "wind_speed_kmh": cur.get("wind_speed_10m"),
        "weather_code": cur.get("weather_code"),
        "grid_latitude": resp.get("latitude"),
        "grid_longitude": resp.get("longitude"),
        "elevation_m": resp.get("elevation"),
        "ingested_at_utc": iso_from_run_ts(doc.get("ingested_at")),
        "source_file": path.name,
    }
    stats = dict(rows_read=1, inserted=0, updated=0, skipped_stale=0, rejected=0)
    errs = validate_weather(w)
    if not errs and not conn.execute("SELECT 1 FROM earthquake_event WHERE event_id=?",
                                     (w["event_id"],)).fetchone():
        errs.append("unknown event_id (event missing or rejected)")
    if errs:
        reject(conn, path.name, w["event_id"], "; ".join(errs), doc)
        stats["rejected"] = 1
        return stats
    if w["weather_code"] is not None:  # tolerate WMO codes we have not seeded
        conn.execute("INSERT OR IGNORE INTO weather_code VALUES (?,?,?)",
                     (w["weather_code"], f"Unmapped WMO code {w['weather_code']}", "Unknown"))
    existing = conn.execute(
        "SELECT ingested_at_utc FROM weather_observation WHERE event_id=? AND observation_time_utc=?",
        (w["event_id"], w["observation_time_utc"])).fetchone()
    conn.execute(UPSERT_WX, [w[c] for c in WX_COLS])
    if existing is None:
        stats["inserted"] = 1
    elif w["ingested_at_utc"] > existing[0]:
        stats["updated"] = 1
    else:
        stats["skipped_stale"] = 1
    return stats


def load_landing(landing, db_path):
    landing = Path(landing)
    conn = connect(db_path)
    init_schema(conn)
    totals = {}
    # events first so weather rows can reference them
    for source_type, loader in (("usgs", load_usgs_file), ("openmeteo", load_weather_file)):
        for path in sorted((landing / source_type).glob("*.json")):
            digest = sha256_file(path)
            if conn.execute("SELECT 1 FROM ingestion_log WHERE sha256=?", (digest,)).fetchone():
                log.info("skip (already loaded): %s", path.name)
                continue
            try:
                st = loader(conn, path)
            except (json.JSONDecodeError, OSError) as exc:
                conn.rollback()
                reject(conn, path.name, None, f"unreadable file: {exc}", {})
                conn.commit()
                log.error("unreadable file %s: %s", path.name, exc)
                continue
            conn.execute(
                "INSERT INTO ingestion_log(source_file, sha256, source_type, rows_read, inserted, "
                "updated, skipped_stale, rejected, loaded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (path.name, digest, source_type, st["rows_read"], st["inserted"], st["updated"],
                 st["skipped_stale"], st["rejected"], now_utc()))
            conn.commit()
            for k, v in st.items():
                totals[k] = totals.get(k, 0) + v
            log.info("loaded %-45s %s", path.name, st)
    conn.close()
    return totals


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--landing", default=str(ROOT / "landing_zone"))
    ap.add_argument("--db", default=str(ROOT / "data" / "quake_weather.db"))
    a = ap.parse_args()
    print(load_landing(a.landing, a.db))
