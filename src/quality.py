"""
quality.py - Data-quality checks over the normalized and star tables.
Exit code 1 if any hard check fails (so the orchestrator can stop the pipeline).

Usage:
    python src/quality.py --db data/quake_weather.db
"""
import argparse
import sys

from load_normalized import ROOT, connect

# (name, severity, SQL returning a count of VIOLATIONS)
CHECKS = [
    ("event_id_not_null_unique", "error",
     "SELECT COUNT(*) - COUNT(DISTINCT event_id) + SUM(event_id IS NULL) FROM earthquake_event"),
    ("magnitude_in_range(-2..10)", "error",
     "SELECT COUNT(*) FROM earthquake_event WHERE magnitude NOT BETWEEN -2 AND 10"),
    ("lat_lon_in_range", "error",
     "SELECT COUNT(*) FROM earthquake_event WHERE latitude NOT BETWEEN -90 AND 90 "
     "OR longitude NOT BETWEEN -180 AND 180"),
    ("depth_in_range(-5..800)", "error",
     "SELECT COUNT(*) FROM earthquake_event WHERE depth_km IS NOT NULL AND depth_km NOT BETWEEN -5 AND 800"),
    ("updated_not_before_event", "error",
     "SELECT COUNT(*) FROM earthquake_event WHERE updated_time_utc < event_time_utc"),
    ("weather_has_parent_event", "error",
     "SELECT COUNT(*) FROM weather_observation w LEFT JOIN earthquake_event e USING(event_id) "
     "WHERE e.event_id IS NULL"),
    ("weather_values_plausible", "error",
     "SELECT COUNT(*) FROM weather_observation WHERE temperature_c NOT BETWEEN -90 AND 60 "
     "OR relative_humidity_pct NOT BETWEEN 0 AND 100 OR wind_speed_kmh < 0 OR precipitation_mm < 0"),
    ("fact_rows_equal_events", "error",
     "SELECT ABS((SELECT COUNT(*) FROM fact_earthquake_weather) - (SELECT COUNT(*) FROM earthquake_event))"),
    ("fact_fk_dim_date", "error",
     "SELECT COUNT(*) FROM fact_earthquake_weather f LEFT JOIN dim_date d USING(date_key) WHERE d.date_key IS NULL"),
    ("fact_fk_dim_location", "error",
     "SELECT COUNT(*) FROM fact_earthquake_weather f LEFT JOIN dim_location d USING(location_key) WHERE d.location_key IS NULL"),
    ("fact_fk_dim_network", "error",
     "SELECT COUNT(*) FROM fact_earthquake_weather f LEFT JOIN dim_network d USING(network_key) WHERE d.network_key IS NULL"),
    ("fact_fk_dim_weather", "error",
     "SELECT COUNT(*) FROM fact_earthquake_weather f LEFT JOIN dim_weather_condition d USING(weather_condition_key) WHERE d.weather_condition_key IS NULL"),
    ("events_without_weather", "warn",
     "SELECT COUNT(*) FROM fact_earthquake_weather WHERE weather_condition_key = -1"),
    ("rejected_records_present", "warn", "SELECT COUNT(*) FROM rejected_record"),
]


def run_checks(db_path, verbose=True):
    conn = connect(db_path)
    results = []
    for name, severity, sql in CHECKS:
        bad = conn.execute(sql).fetchone()[0] or 0
        status = "PASS" if bad == 0 else ("FAIL" if severity == "error" else "WARN")
        results.append((name, severity, bad, status))
        if verbose:
            print(f"{status:5} {name:34} violations={bad}")
    conn.close()
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=str(ROOT / "data" / "quake_weather.db"))
    res = run_checks(ap.parse_args().db)
    sys.exit(1 if any(r[3] == "FAIL" for r in res) else 0)
