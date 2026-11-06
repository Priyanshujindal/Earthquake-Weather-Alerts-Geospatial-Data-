"""
Generates SYNTHETIC sample files in the exact USGS GeoJSON / Open-Meteo formats,
used for offline demos and unit tests. They are NOT real observations.
Run:  python tests/make_sample_data.py
"""
import json
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(__file__).resolve().parent / "sample_data"


def ms(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)


def feature(eid, mag, place, lon, lat, depth, t, upd, net, magtype, sig=None, typ="earthquake"):
    return {
        "type": "Feature",
        "properties": {
            "mag": mag, "place": place, "time": ms(t), "updated": ms(upd), "tz": None,
            "url": f"https://earthquake.usgs.gov/earthquakes/eventpage/{eid}",
            "detail": f"https://earthquake.usgs.gov/fdsnws/event/1/query?eventid={eid}&format=geojson",
            "felt": None, "cdi": None, "mmi": None, "alert": None, "status": "reviewed",
            "tsunami": 0, "sig": sig if sig is not None else int(max(mag, 0) ** 2 * 10),
            "net": net, "code": eid[len(net):], "ids": f",{eid},", "sources": f",{net},",
            "types": ",origin,phase-data,", "nst": 20, "dmin": 0.05, "rms": 0.2, "gap": 80,
            "magType": magtype, "type": typ, "title": f"M {mag} - {place}",
        },
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
        "id": eid,
    }


def collection(features, generated):
    return {
        "type": "FeatureCollection",
        "metadata": {"generated": ms(generated), "title": "USGS All Earthquakes, Past Day",
                     "status": 200, "api": "1.14.1", "count": len(features)},
        "features": features,
        "bbox": [-180, -90, 0, 180, 90, 700],
    }


def weather(eid, lat, lon, run_ts, obs, seed):
    return {
        "quake_id": eid, "ingested_at": run_ts,
        "response": {
            "latitude": lat, "longitude": lon, "generationtime_ms": 0.05, "utc_offset_seconds": 0,
            "timezone": "UTC", "timezone_abbreviation": "UTC", "elevation": 100.0 + seed * 7,
            "current_units": {"time": "iso8601", "interval": "seconds", "temperature_2m": "°C",
                              "relative_humidity_2m": "%", "precipitation": "mm",
                              "wind_speed_10m": "km/h", "weather_code": "wmo code"},
            "current": {"time": obs, "interval": 900,
                        "temperature_2m": round(8 + (seed * 3.7) % 24, 1),
                        "relative_humidity_2m": 35 + (seed * 11) % 60,
                        "precipitation": round(((seed * 7) % 5) * 0.2, 1),
                        "wind_speed_10m": round(3 + (seed * 2.9) % 25, 1),
                        "weather_code": [0, 2, 3, 61, 45, 80, 95][seed % 7]},
        },
    }


RUN1, RUN2 = "20261105T120000Z", "20261106T060000Z"

run1 = [
    feature("ci40123456", 2.8, "9 km SW of Ridgecrest, CA", -117.70, 35.57, 6.2, "2026-11-05 03:10:11", "2026-11-05 03:25:00", "ci", "ml"),
    feature("us7000sample2", 4.6, "Mid-Atlantic Ridge", -29.10, 0.80, 10.0, "2026-11-05 05:42:30", "2026-11-05 06:10:00", "us", "mb"),
    feature("us7000sample1", 5.4, "45 km NE of Hirara, Japan", 125.60, 25.10, 35.0, "2026-11-05 07:15:02", "2026-11-05 08:00:00", "us", "mww"),
    feature("nc75000001", 1.9, "5 km NNW of The Geysers, CA", -122.80, 38.80, 2.1, "2026-11-05 08:30:45", "2026-11-05 08:45:00", "nc", "md"),
    feature("ak0250sample", 3.1, "62 km SSE of Pedro Bay, Alaska", -153.20, 59.30, 95.4, "2026-11-05 09:05:19", "2026-11-05 09:30:00", "ak", "ml"),
    feature("hv74000001", 2.2, "7 km ESE of Volcano, Hawaii", -155.17, 19.40, 4.5, "2026-11-05 10:20:00", "2026-11-05 10:40:00", "hv", "ml"),
]
run2 = [
    # revised event: same id, NEWER updated, magnitude + magType changed -> must overwrite
    feature("us7000sample2", 4.8, "Mid-Atlantic Ridge", -29.10, 0.80, 10.0, "2026-11-05 05:42:30", "2026-11-06 02:00:00", "us", "mww"),
    # unchanged repeat of a run-1 event -> must be skipped as stale
    run1[0],
    feature("us7000sample3", 6.1, "112 km W of Kandrian, Papua New Guinea", 148.30, -6.30, 55.0, "2026-11-06 01:12:40", "2026-11-06 02:30:00", "us", "mww"),
    feature("ci40123457", 3.4, "14 km N of Borrego Springs, CA", -116.40, 33.40, 8.0, "2026-11-06 03:33:03", "2026-11-06 03:50:00", "ci", "ml"),
    feature("nn00890001", 2.5, "28 km E of Mina, Nevada", -117.60, 38.40, 9.9, "2026-11-06 04:44:44", "2026-11-06 05:00:00", "nn", "ml"),
    # deliberately invalid -> must be rejected, not loaded
    feature("bad0001", 14.2, "Nowhere, Testland", 10.0, 10.0, 5.0, "2026-11-06 05:10:00", "2026-11-06 05:20:00", "us", "mww"),
]

for sub in ("usgs", "openmeteo"):
    (OUT / sub).mkdir(parents=True, exist_ok=True)

for run_ts, feats, gen in ((RUN1, run1, "2026-11-05 12:00:00"), (RUN2, run2, "2026-11-06 06:00:00")):
    (OUT / "usgs" / f"usgs_all_day_{run_ts}.json").write_text(json.dumps(collection(feats, gen), indent=2))
    obs = datetime.strptime(run_ts, "%Y%m%dT%H%M%SZ").strftime("%Y-%m-%dT%H:%M")
    for i, f in enumerate(feats):
        lon, lat, _ = f["geometry"]["coordinates"]
        w = weather(f["id"], lat, lon, run_ts, obs, i + (0 if run_ts == RUN1 else 3))
        (OUT / "openmeteo" / f"weather_{f['id']}_{run_ts}.json").write_text(json.dumps(w, indent=2))
print("sample data written to", OUT)
