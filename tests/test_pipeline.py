import shutil
import sqlite3
from pathlib import Path

import pytest

import load_normalized as ln
import quality
import transform_star

SAMPLE = Path(__file__).resolve().parent / "sample_data"


@pytest.fixture()
def db(tmp_path):
    landing = tmp_path / "landing"
    shutil.copytree(SAMPLE, landing)
    path = tmp_path / "t.db"
    ln.load_landing(landing, path)
    transform_star.build_star(path)
    return path, landing


def q(path, sql, *args):
    c = sqlite3.connect(path)
    try:
        return c.execute(sql, args).fetchall()
    finally:
        c.close()


def test_parse_region():
    assert ln.parse_region("10 km SSW of Ridgecrest, CA") == "CA"
    assert ln.parse_region("45 km NE of Hirara, Japan") == "Japan"
    assert ln.parse_region("Mid-Atlantic Ridge") == "Mid-Atlantic Ridge"
    assert ln.parse_region(None) == "Unknown"


def test_event_counts_and_rejects(db):
    path, _ = db
    assert q(path, "SELECT COUNT(*) FROM earthquake_event")[0][0] == 9     # 6 + 3 new valid
    rej = q(path, "SELECT record_key FROM rejected_record ORDER BY record_key")
    assert [r[0] for r in rej] == ["bad0001", "bad0001"]                   # event + its weather


def test_dedup_newer_update_wins(db):
    path, _ = db
    mag, mt = q(path, "SELECT magnitude, mag_type FROM earthquake_event WHERE event_id='us7000sample2'")[0]
    assert (mag, mt) == (4.8, "mww")


def test_stale_repeat_not_duplicated(db):
    path, _ = db
    assert q(path, "SELECT COUNT(*) FROM earthquake_event WHERE event_id='ci40123456'")[0][0] == 1
    skipped = q(path, "SELECT skipped_stale FROM ingestion_log WHERE source_file LIKE 'usgs_all_day_20261106%'")[0][0]
    assert skipped == 1


def test_weather_observations_accumulate(db):
    path, _ = db
    assert q(path, "SELECT COUNT(*) FROM weather_observation")[0][0] == 11
    assert q(path, "SELECT COUNT(*) FROM weather_observation WHERE event_id='ci40123456'")[0][0] == 2


def test_load_is_idempotent(db):
    path, landing = db
    before = q(path, "SELECT COUNT(*) FROM earthquake_event")[0][0]
    totals = ln.load_landing(landing, path)
    assert totals == {}                                                    # every file skipped
    assert q(path, "SELECT COUNT(*) FROM earthquake_event")[0][0] == before


def test_star_schema(db):
    path, _ = db
    assert q(path, "SELECT COUNT(*) FROM fact_earthquake_weather")[0][0] == 9
    assert q(path, "SELECT COUNT(*) FROM dim_date")[0][0] == 2             # Nov 5 + Nov 6
    cls = q(path, "SELECT c.class_name FROM fact_earthquake_weather f "
                  "JOIN dim_magnitude_class c USING(magnitude_class_key) WHERE event_id='us7000sample3'")
    assert cls[0][0] == "Major"                                            # M6.1
    # latest weather observation is used for the repeated event
    obs = q(path, "SELECT weather_observed_at_utc FROM fact_earthquake_weather WHERE event_id='ci40123456'")
    assert obs[0][0].startswith("2026-11-06")


def test_quality_checks_pass(db):
    path, _ = db
    results = quality.run_checks(path, verbose=False)
    assert not [r for r in results if r[3] == "FAIL"]
