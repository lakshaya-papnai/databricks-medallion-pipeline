# Code Review Notes

This document outlines the code review process applied to all AI-generated code before accepting it into the project. It covers the general methodology, layer-specific observations, and areas for improvement in a full production setting.

## Review Methodology

Before saving and running any AI-generated code, I performed a manual review of each file. The review process focused on the following criteria:
- **Rule Adherence:** Did the code strictly follow the hard rules defined in `.cursorrules`?
- **Schema Validation:** Did the implementation and data types match the specifications in `data-model.md`?
- **Intentional Quality Issues:** Did the code correctly handle and flag the planted data quality issues (our test oracle) without breaking or deleting rows?
- **Execution Viability:** Were there any obvious syntax errors, unsupported functions, or logic flaws that would prevent the script from running successfully?

## Bronze Layer Review Observations

- **Explicit schema enforcement on read:** Verified that all column types defined during read operations exactly match the schema in `data-model.md`.
- **Metadata columns:** Ensured that `ingestion_timestamp` and `source_file_name` were present on all 3 tables.
- **No transformations applied:** Confirmed. that raw data was preserved exactly as-is, including all NULLs and duplicates.
- **Validation:** Checked that 50 NULL emails and 10 duplicate `customer_ids` were preserved in the output.

## Silver Layer Review Observations

- **Quality flag preservation:** Verified that the `quality_check_result` column is present on all output tables and that the layer never deletes rows.
- **Combiner logic:** Confirmed that the `concat_ws()` pattern correctly combines multiple failure reasons on one row.
- **Referential Integrity constraints:** The referential integrity check correctly skips rows where the FK is already NULL — those belong to the completeness check, not RI. I caught this in review and confirmed the `isNotNull()` condition was present.
- **Join Fan-Out Prevention:** `create_silver_tables.py` joins on both PK + `ingestion_timestamp` — I reviewed this specifically to prevent fan-out with duplicate rows. *Note: minor fan-out still observed in final counts (10,150 vs 10,010) — documented in `debugging-notes.md`.*
- **Windowing Logic:** The `ROW_NUMBER()` window function is ordered by `ingestion_timestamp` so the first occurrence = PASS, and subsequent occurrences = FAIL.

## Gold Layer Review Observations

- **PASS filtering:** All Gold scripts filter silver tables to PASS rows only before aggregating — I verified `WHERE quality_check_result = 'PASS'` (or its DataFrame equivalent) is present in all scripts.
- **Percentile Ranking:** `PERCENT_RANK()` is used for High-Value segmentation, not `NTILE()` — I reviewed this specifically, as `NTILE()` creates equal buckets which is wrong for true percentile ranking.
- **Join types:** The `LEFT JOIN` in segmentation ensures Inactive customers (0 orders) are included — I reviewed the join type explicitly.
- **NULL Handling:** `COALESCE(revenue, 0)` is present for customers with no completed orders — I checked that this prevents NULL revenues in segmentation.
- **Segmentation Priority:** The priority order in `CASE WHEN` is High-Value → Inactive → Repeat → One-Time — I reviewed this because it matters; a high-value customer with 2+ orders should be categorized as High-Value, not Repeat.

## Test Review Observations

- **Exact assertions:** The 9 test assertions each check exact expected counts, not approximate ranges.
- **Test Oracle:** The tests use the planted quality issue counts as an oracle — they use the same numbers from `generate_sample_data.py`.
- **Local Pass Rate:** All 9 tests passed on the local run — this was confirmed by `pipeline_run.log`.

## What I Would Improve in a Production Code Review

- Add type hints to all Python functions
- Break `create_silver_tables.py` into smaller testable functions
- Add schema validation assertions at the start of each script
- Replace hardcoded paths with a central config file
