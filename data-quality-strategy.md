# Data Quality Strategy

## Architecture Overview
The data quality strategy is integrated directly into the Medallion architecture (Bronze → Silver → Gold). Data quality is not an afterthought, but a core responsibility of the pipeline:
- **Bronze Layer**: Preserves all data exactly as it arrives, including all errors, duplicates, and missing values.
- **Silver Layer**: Acts as the data quality engine. It identifies, validates, and flags anomalous data without deleting it.
- **Gold Layer**: Acts as the quality gatekeeper. It strictly filters out any flagged data to ensure dashboards are built on a pristine dataset.

## Data Model & Schema
The data model contains specific, intentionally planted quality issues to validate the pipeline:
- **customers**: 10,010 rows. Contains exactly 50 NULL emails and 10 duplicate `customer_id`s.
- **orders**: 100,020 rows. Contains exactly 100 NULL `customer_id`s, 200 NULL `product_id`s, 50 orphan `customer_id`s, 30 orphan `product_id`s, and 20 duplicate `order_id`s.
- **products**: 500 rows. Clean data with no intentional issues.
- **quality_check_result**: The single schema addition in the Silver layer that underpins the entire strategy. It holds either `'PASS'` or a concatenated string of failure reasons (e.g., `'FAIL - NULL customer_id, NULL product_id'`).

## Bronze Layer Design
- **No cleaning or filtering**: The Bronze layer's data quality role is strictly passive. It must not drop NULLs, deduplicate rows, or apply schema inference that might alter source data.
- **Metadata for Lineage**: Adding `ingestion_timestamp` and `source_file_name` ensures that when bad data is identified in Silver, its exact origin and load time are known.

## Silver Layer Design
- **Never delete bad rows**: Deleting data destroys auditability and lineage. Instead, Silver evaluates every row and assigns a `quality_check_result`.
- **Composite Flags**: When a single row fails multiple checks (e.g., it is both a duplicate and has a NULL foreign key), `concat_ws` is used to combine all failure reasons into a single string. This ensures no quality issue is hidden behind another.
- **Independent Check Execution**: Each data quality check runs as an independent script, producing an intermediate table. These are then orchestrated and joined together on a compound key `(primary_key, ingestion_timestamp)` to prevent row fan-out during the final flag consolidation.

## Gold Layer Design
- **Strict Quality Gating**: The Gold layer must unconditionally apply `.filter("quality_check_result = 'PASS'")` before performing any aggregations.
- **Protection of Business Metrics**: By enforcing the PASS filter, we guarantee that orphan records, negative amounts, or missing categories do not contaminate the final revenue metrics or customer segmentation logic displayed on the dashboard.

## Data Quality Validation Strategy
We implemented five rigorous quality checks in the Silver layer.
1. **Completeness**: Checks critical fields for NULLs (e.g., `email`, `customer_id`, `product_id`). 
2. **Uniqueness**: Uses `ROW_NUMBER()` to identify duplicate primary keys. *Note: Threshold for acceptable data = 100% unique; the pass rate in the report will reflect the actual duplicate count.*
3. **Referential Integrity**: Uses `LEFT JOIN`s to ensure foreign keys exist in their parent dimension tables. (Only evaluates if the foreign key is NOT NULL).
4. **Type Validation & Domain Integrity**: Validates that numeric fields are non-negative and dates are structurally valid.
5. **Business Logic**: Evaluates cross-column arithmetic (e.g., `quantity * unit_price == total_amount`) and business rules (e.g., `payment_date` must not be before `order_date`).

**Quality Metrics Report**
A summary report is printed at the end of the Silver layer processing to track these checks:
| check_name | total_rows | passed_rows | failed_rows | pass_percentage |
| :--- | :--- | :--- | :--- | :--- |
| Completeness - Customer Email | 10010 | 9960 | 50 | 99.50% |
| Uniqueness - Customer ID | 10010 | 10000 | 10 | 99.90% |

*(Failed rows exactly match the planted intentional quality issues).*

## Debugging Approach
- **Test Oracles**: The exact counts of planted issues (e.g., 50 orphan customer IDs) serve as test oracles. If the Silver pipeline flags 49 or 51 orphans, the check logic is broken.
- **In-script Validation**: Every validation output prints the actual count alongside the expected count: `Orphan customer_ids: 50 (expected: 50)`. Discrepancies are immediately obvious.
- **Intermediate Tables**: If the final consolidated Silver table has an incorrect row count or flag distribution, the intermediate per-check tables (`silver_customers_completeness`, etc.) are queried individually to isolate the specific check that failed.
