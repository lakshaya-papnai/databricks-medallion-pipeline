# Requirements Analysis: Databricks Medallion Architecture Data Pipeline

## 1. Problem Statement
The e-commerce company receives daily sales data in raw CSV format across three domains: customers, orders, and products. To enable accurate and scalable business intelligence, the company requires an automated data pipeline using a Databricks Medallion architecture (Bronze, Silver, Gold layers). The data contains intentional quality issues (duplicates, nulls, orphans) which must be identified, flagged, and filtered out of final business reporting without actually deleting any records from the underlying tables.

## 2. Functional Requirements

### Bronze Layer (Raw Ingestion)
*   **Objective:** Ingest raw CSV files (`customers.csv`, `orders.csv`, `products.csv`) exactly as they are received.
*   **Actions:**
    *   Load raw data into Delta tables enforcing explicit schemas (no schema inference).
    *   No cleaning, filtering, or transformations — raw data only. All NULLs and duplicate rows must be preserved exactly as they are.
    *   Append metadata columns: `ingestion_timestamp` (current timestamp) and `source_file_name` for lineage.
    *   Write as Delta tables using `mode("overwrite")` to paths like `/FileStore/delta/bronze/...`.

### Silver Layer (Cleansed and Conformed)
*   **Objective:** Perform data quality checks and flag bad data, serving as a reliable single source of truth without losing any source rows.
*   **Actions:**
    *   **Strict Rule:** Read from Bronze Delta tables only, never from raw CSVs.
    *   **Strict Rule:** Never delete bad rows. Instead, add a `quality_check_result` column. Rows passing all checks get `'PASS'`, failing rows get `'FAIL - <reason>'`.
    *   If a row fails multiple checks, failure reasons must be concatenated into a single string (e.g., using `concat_ws`).
    *   **Checks required:** 
        *   Completeness (flagging NULL foreign keys and emails)
        *   Uniqueness (using `ROW_NUMBER()` to flag duplicates)
        *   Type validation / Domain integrity (e.g., numeric ranges, cost < price)
        *   Referential integrity (flagging orphaned foreign keys)
        *   Business logic (e.g., total_amount calculation checks, date validity)
    *   Write final Silver tables using `mode("overwrite")`.

### Gold Layer (Business Aggregates)
*   **Objective:** Transform quality-checked data into presentation-ready datasets optimized for downstream dashboards.
*   **Actions:**
    *   **Strict Rule:** Read from final Silver Delta tables only, and ONLY include rows where `quality_check_result = 'PASS'`.
    *   Write pure SQL aggregations via `spark.sql()` using temporary views.
    *   Round all monetary values to 2 decimal places.
    *   Produce specific reporting tables: Sales by Product, Revenue by Customer, Daily/Weekly Trends, and Customer Segmentation.
    *   Write as Delta tables using `mode("overwrite")`.

## 3. Dashboard and Reporting Requirements
*   **Objective:** Provide a Databricks SQL Dashboard answering key business questions.
*   **Dashboards must cover:**
    1.  **Top 10 Products by Revenue:** Bar chart comparing product revenue across categories.
    2.  **Customer Revenue Distribution:** Histogram/Bar chart grouping customers into defined spend buckets.
    3.  **Customer Segmentation Breakdown:** Pie/Donut chart showing proportion of High-Value, Repeat, One-Time, and Inactive customers.
    4.  **Weekly Revenue Trend:** Line chart showing week-over-week revenue and YTD cumulative growth.
    5.  **Monthly Revenue by Customer Segment:** Stacked bar chart showing monthly trends per segment.

## 4. Technical and Architectural Rules
*   **Environment:** Databricks Community Edition using PySpark, Delta Lake, and Databricks SQL.
*   **Storage:** DBFS for storage (`/FileStore/tables/` for CSVs, `/FileStore/delta/` for Delta tables).
*   **Orchestration:** Each layer (Bronze, Silver, Gold) must have an orchestrator script (`ingest_all.py`, `create_silver_tables.py`, `create_gold_tables.py`) that runs its respective scripts in the correct dependency order.
*   **Error Handling:** Orchestrators must use a "fail-and-continue" pattern (try/except) so a single script failure doesn't block the rest of the layer, but must exit with a non-zero code (`sys.exit(1)`) on partial failure to signal downstream job monitors.
*   **Code Quality:** All code must be well-commented, modular (e.g., using `importlib` for sequential script execution), and structured in a production-ready format.

## 5. Known Data Quality Issues to Handle (Planted)
*   **Customers:** 50 NULL emails, 10 duplicate `customer_ids`.
*   **Orders:** 100 NULL `customer_ids`, 200 NULL `product_ids`, 50 orphan `customer_ids`, 30 orphan `product_ids`, 20 duplicate `order_ids`.
*   **Products:** 500 clean rows (no intentional issues).

## 6. Acceptance Criteria

*   **Bronze Layer:** 
    *   Record counts and NULL/duplicate profiles in Bronze Delta tables exactly match the raw CSV data.
    *   `ingestion_timestamp` and `source_file_name` are accurately populated.
*   **Silver Layer:**
    *   Final Silver tables contain the exact same number of rows as Bronze (no data deleted).
    *   `quality_check_result` accurately flags all planted data quality issues with the correct string format.
*   **Gold Layer:**
    *   Aggregations only include `PASS` rows.
    *   Monetary values are rounded correctly to 2 decimal places.
    *   Table schemas match the exact requirements of the downstream Databricks SQL dashboard queries.
