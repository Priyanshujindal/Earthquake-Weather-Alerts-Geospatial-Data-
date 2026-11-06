"""
transform_star.py - Build the star schema from the normalized tables (full refresh).

Usage:
    python src/transform_star.py --db data/quake_weather.db
"""
import argparse
import logging
from pathlib import Path

from load_normalized import ROOT, connect

log = logging.getLogger("star")


def build_star(db_path):
    conn = connect(db_path)
    conn.executescript((ROOT / "sql" / "create_star_schema.sql").read_text())
    conn.executescript((ROOT / "sql" / "transform_star.sql").read_text())
    conn.commit()
    counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in ("dim_date", "dim_time", "dim_location", "dim_network",
                        "dim_magnitude_class", "dim_weather_condition", "fact_earthquake_weather")}
    conn.close()
    log.info("star schema built: %s", counts)
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=str(ROOT / "data" / "quake_weather.db"))
    print(build_star(ap.parse_args().db))
