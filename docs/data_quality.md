# Data quality

| Check | Layer | On failure |
|---|---|---|
| Event id present & unique | Silver / normalized | Reject row / fail job |
| Magnitude between -2 and 10 | Silver | Reject row → quarantine |
| Latitude ∈ [-90, 90], longitude ∈ [-180, 180] | Silver | Reject row |
| Depth ∈ [-5, 800] km | Silver | Reject row |
| `updated` ≥ event time | Normalized | Fail |
| Weather has a parent event | Silver | Reject row |
| Temperature ∈ [-90, 60] °C, humidity ∈ [0, 100], wind/precip ≥ 0 | Silver | Reject row |
| Fact rows == event rows | Gold | Fail job |
| No orphan foreign keys (6 dimensions) | Gold | Fail job |
| Events without weather | Gold | Warn |
| Rejected record count | Silver | Warn |

* **Local:** `python src/quality.py` (non-zero exit on failure). **Databricks:** `05_data_quality.py` writes `dq_results` and raises to fail the Workflow.
* **Schema evolution:** Bronze keeps raw JSON; Silver parses with an explicit schema, so new upstream fields are ignored safely and removed fields become NULLs that the validation rules catch.
