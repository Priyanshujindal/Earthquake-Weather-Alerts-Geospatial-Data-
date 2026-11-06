"""
run_pipeline.py - Run the whole LOCAL pipeline: ingest -> load 3NF -> star -> quality.
(The Databricks version of the same flow lives in /databricks.)

Examples:
    python src/run_pipeline.py                                   # live APIs
    python src/run_pipeline.py --skip-ingest --landing tests/sample_data   # offline demo
"""
import argparse
import sys

import ingest
import load_normalized as ln
import quality
import transform_star


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--feed", default="all_day")
    ap.add_argument("--max-quakes", type=int, default=25)
    ap.add_argument("--landing", default=str(ln.ROOT / "landing_zone"))
    ap.add_argument("--db", default=str(ln.ROOT / "data" / "quake_weather.db"))
    ap.add_argument("--skip-ingest", action="store_true", help="Use files already in --landing")
    a = ap.parse_args(argv)

    if not a.skip_ingest:
        ingest.run(a.feed, a.max_quakes, landing=a.landing)
    print("load:", ln.load_landing(a.landing, a.db))
    print("star:", transform_star.build_star(a.db))
    results = quality.run_checks(a.db)
    return 1 if any(r[3] == "FAIL" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
