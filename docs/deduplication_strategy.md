# Deduplication strategy

USGS feeds are **overlapping and mutable**: the same earthquake appears in every feed snapshot until it ages out, and its
magnitude/location/status can be revised hours or days later (e.g. automatic M4.6 → reviewed M4.8).

## Keys
| Entity | Natural (dedup) key | Version column |
|---|---|---|
| `earthquake_event` | `event_id` (USGS `id`, stable) | `updated_time_utc` (USGS `properties.updated`) |
| `weather_observation` | `(event_id, observation_time_utc)` | `ingested_at_utc` |
| source file | `sha256` of file bytes | – |

## Rules
1. **File level (idempotency).** Every landing file is hashed; hashes live in `ingestion_log` (local) / Bronze `_source_file` (Databricks). Re-running the loader on the same files does nothing.
2. **Event level (upsert, latest version wins).**
   * New `event_id` → **insert**.
   * Existing `event_id` and incoming `updated` **>** stored `updated` → **update** all attributes (magnitude revisions flow through).
   * Existing `event_id` and incoming `updated` **≤** stored → **skip** (stale or identical repeat) and count as `skipped_stale`.
3. **Within a single batch** duplicates are collapsed first with `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated DESC, _ingested_at DESC) = 1`.
4. **Weather level.** One new observation row per run per event (history accumulates); identical `(event_id, observation_time)` pairs are collapsed, keeping the newest ingestion. The star schema reads only the *latest* observation per event.
5. **Rejects are never silently dropped** – invalid rows go to `rejected_record` / `silver_rejected_records` with a reason.

## Implementation
* SQLite: `INSERT ... ON CONFLICT(event_id) DO UPDATE ... WHERE excluded.updated_time_utc > earthquake_event.updated_time_utc` (`src/load_normalized.py`).
* Delta Lake: `MERGE INTO ... WHEN MATCHED AND s.updated_time_utc > t.updated_time_utc THEN UPDATE * WHEN NOT MATCHED THEN INSERT *` (`databricks/03_silver.py`).

## Proof
`tests/test_pipeline.py` loads two overlapping snapshots and asserts: revised event updated (M4.6 → M4.8), unchanged repeat skipped, no duplicate `event_id`, re-load is a no-op.
