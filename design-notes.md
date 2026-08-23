# Design Notes

## Architecture Overview
The Medallion architecture (Bronze → Silver → Gold) was chosen because it separates three fundamentally different concerns into distinct layers, each with a clear and non-overlapping responsibility:
- **Bronze Layer**: Raw ingest, no transformations, complete audit trail.
- **Silver Layer**: Quality flagging, validation, enrichment.
- **Gold Layer**: Business aggregations, analytics-ready.
- **Dashboard**: Visualisations for business stakeholders based on Gold tables.

By separating layers, each stage can be tested and validated independently. Every layer reads from and writes to Delta Lake, ensuring consistency and auditability across the full chain.

## Data Model & Schema
The data pipeline processes three primary entities:
- **customers**: Contains customer demographics and segments (10,010 rows). Intentionally contains NULL emails and duplicate customer IDs.
- **orders**: Contains transactional data linking customers to products (100,020 rows). Includes intentional NULL customer/product IDs, orphan IDs, and duplicate order IDs.
- **products**: Contains product catalogue data (500 rows). Clean data with no intentional issues.

## Bronze Layer Design
**Responsibility:** Accept raw data from source CSVs exactly as-is. No cleaning. No filtering. No transformations.
- **Explicit Schema Enforcement:** We use explicit `StructType` schemas rather than `inferSchema=True`. This prevents silent type changes and surfaces errors at ingest time.
- **Metadata Columns:** Added `ingestion_timestamp` and `source_file_name` to every row for auditing.
- **Preserve Everything:** NULL values, duplicate rows, and orphaned foreign keys are written to Bronze exactly as they appear in the source CSV.
- **Mode:** All Bronze tables use `mode("overwrite")`.

## Silver Layer Design
**Responsibility:** Apply data quality checks and flag every row with the result. Bad rows are never deleted.
- **Quality Flags:** Every row receives a `quality_check_result` of `'PASS'` or `'FAIL - <reason(s)>'`.
- **Concat_ws:** For rows failing multiple checks, reasons are concatenated using `concat_ws(", ", ...)` to build a single composite string (e.g., `'FAIL - NULL customer_id, NULL product_id'`).
- **Compound Join Key:** When combining intermediate quality check tables, joining on primary keys alone risks fan-out due to duplicates. We join on `(primary_key, ingestion_timestamp)` to uniquely identify physical rows.
- **LEFT JOIN for Referential Integrity:** Used to find orphans without dropping records. We only check for orphans if the foreign key `IS NOT NULL` (handled by the completeness check).

## Gold Layer Design
**Responsibility:** Produce clean, aggregated, business-ready tables from Silver PASS rows only.
- **PASS Rows Only:** All aggregations apply `.filter("quality_check_result = 'PASS'")` before any calculation.
- **Aggregations:** Includes `gold_sales_by_product`, `gold_revenue_by_customer`, `gold_customer_segmentation`, `gold_daily_trends`, and `gold_weekly_trends`.
- **SQL with Temp Views:** Aggregations use `spark.sql()` with registered temp views for readable, SQL-native business logic instead of chained PySpark methods.
- **Segment Rank:** Used `PERCENT_RANK() OVER (ORDER BY total_revenue DESC)` for High-Value segmentation to properly capture the continuous revenue distribution, rather than discrete equal-sized buckets like `NTILE(5)`.

## Data Quality Validation Strategy
Validation is treated as a non-negotiable step at every layer.
- **In-script Validation:** Every script ends with a validation section that reads back from the newly written Delta table (not memory) and prints row counts, NULL counts, and pass/fail splits.
- **Test Oracles:** Intentional data quality issues were planted (e.g., exactly 50 NULL emails, 20 duplicate order IDs) to act as a test oracle.
- **Expected vs Actual:** Validation prints use the format `(expected: 50)` next to the actual count to make discrepancies immediately visible.

## Debugging Approach
Errors in this pipeline were traced layer by layer:
- **Validating at Boundaries:** Checking DataFrames after reading back from Delta prevents downstream propagation of errors.
- **Intermediate Checkpoints:** During Silver processing, intermediate check tables are written before combining. If the final count is wrong, intermediate tables are inspected individually.
- **Floating-point fixes:** Used `spark_round()` to fix floating-point arithmetic errors during business logic checks.
- **Compound Join Keys:** Caught and fixed row fan-out during Silver orchestration by joining on both primary key and ingestion timestamp.
