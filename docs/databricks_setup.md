# Databricks runbook

## 1. Get the code into Databricks
**Workspace → Create → Git folder** → paste your GitHub repo URL. (Or upload the `databricks/` folder.)

## 2. Run once manually (in order)
`01_ingest_to_landing` → `02_bronze` → `03_silver` → `04_gold` → `05_data_quality`
Compute: **Serverless** (or any cluster with DBR 14+). Catalog/schema default to `workspace.quake_weather`
(edit the widgets in `00_config` to change). Unity Catalog must be enabled (default on current workspaces).

> **Internet blocked?** Upload `tests/sample_data/usgs/*.json` and `tests/sample_data/openmeteo/*.json` to the Volume
> `Catalog → <catalog> → <schema> → landing` (create subfolders `usgs/`, `openmeteo/`), then skip notebook 01.
> Those samples are synthetic – use real API data for your final demo if your workspace allows it.

## 3. Automate with a Workflow
**Option A – UI:** Jobs & Pipelines → Create job → add the 5 notebook tasks with dependencies ingest → bronze → silver → gold → data_quality → add schedule (e.g. hourly) → failure email.
**Option B – CLI:** edit `<your-email>` in `databricks/job.json`, then
`databricks jobs create --json @databricks/job.json`. The schedule ships **PAUSED**; un-pause it for the demo/production.

## 4. Build the dashboard
Dashboards → Create dashboard → Data → *Create from SQL* → paste each query from `dashboard/dashboard_queries.sql`
→ Canvas: add counter (query 1), line (2), bar (3, 4, 6, 8), point map (5), pie (7), table (9).
Publish the dashboard and screenshot it for the README (`docs/images/`).

## 5. Demo script (5 minutes)
| Time | Show |
|---|---|
| 0:00–0:40 | Problem + architecture diagram (`docs/architecture.md`) |
| 0:40–1:30 | Run `python src/ingest.py --max-quakes 5` live → show files in `landing_zone/` |
| 1:30–2:15 | ER diagram → `python src/load_normalized.py` → query `earthquake_event`, show dedup (`ingestion_log`, `rejected_record`) |
| 2:15–3:00 | Star schema diagram → query `fact_earthquake_weather` with dims |
| 3:00–4:00 | Databricks: run/refresh the Workflow, show Bronze → Silver → Gold tables, `dq_results` |
| 4:00–5:00 | Dashboard insights (top regions, magnitude vs weather, map) + wrap-up |
