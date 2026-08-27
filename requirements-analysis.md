# Requirements Analysis: Databricks Medallion Architecture Data Pipeline

## 1. Problem Statement
Basically, we have an e-commerce company getting daily sales data in raw CSVs for customers, orders, and products. To get this ready for dashboards, we need an automated pipeline using a Databricks Medallion architecture (Bronze, Silver, Gold). The raw data is intentionally messy (duplicates, nulls, orphans). The goal is to identify, flag, and filter out all that bad data before it hits the final reporting layer — but without actually deleting any rows from the tables.

## 2. Functional Requirements

### Bronze Layer (Raw Ingestion)
*   **Goal:** Just get the raw CSV files (`customers.csv`, `orders.csv`, `products.csv`) loaded exactly as they are.
*   **Actions:**
    *   Load the data into Delta tables and enforce explicit schemas (don't rely on schema inference).
    *   No cleaning or filtering at all. Keep all the NULLs and duplicates exactly as they arrived.
    *   Add a couple of metadata columns so we can track lineage: `ingestion_timestamp` and `source_file_name`.
    *   Write everything as Delta tables using `mode("overwrite")` to paths like `/FileStore/delta/bronze/...`.

### Silver Layer (Cleansed and Conformed)
*   **Goal:** Run the data quality checks and flag the bad rows. This acts as our single source of truth, and we can't lose any source rows here.
*   **Actions:**
    *   **Strict Rule:** Only read from the Bronze Delta tables, never go back to the raw CSVs.
    *   **Strict Rule:** Never delete bad rows. Instead, just add a `quality_check_result` column. Good rows get `'PASS'`, bad rows get `'FAIL - <reason>'`.
    *   If a row fails multiple checks, we need to concatenate the reasons into a single string (using something like `concat_ws`).
    *   **Checks required:** 
        *   Completeness (flag NULL foreign keys and emails)
        *   Uniqueness (use `ROW_NUMBER()` to flag duplicates)
        *   Type validation (check numeric ranges, like making sure cost < price)
        *   Referential integrity (flag orphaned foreign keys)
        *   Business logic (check total_amount math, validate dates)
    *   Write out the final Silver tables using `mode("overwrite")`.

### Gold Layer (Business Aggregates)
*   **Goal:** Turn the quality-checked data into clean, aggregated tables for the dashboards.
*   **Actions:**
    *   **Strict Rule:** Only read from Silver Delta tables, and strictly filter for `quality_check_result = 'PASS'`.
    *   Do the aggregations in pure SQL using `spark.sql()` and temporary views.
    *   Make sure to round all monetary values to 2 decimal places.
    *   Create specific reporting tables: Sales by Product, Revenue by Customer, Daily/Weekly Trends, and Customer Segmentation.
    *   Write them out as Delta tables using `mode("overwrite")`.

## 3. Dashboard Requirements
*   **Goal:** Build a Databricks SQL Dashboard that actually answers business questions.
*   **Dashboards must include:**
    1.  **Top 10 Products by Revenue:** A bar chart comparing revenue across product categories.
    2.  **Customer Revenue Distribution:** A histogram or bar chart grouping customers into spend buckets.
    3.  **Customer Segmentation Breakdown:** A pie/donut chart showing the split between High-Value, Repeat, One-Time, and Inactive customers.
    4.  **Weekly Revenue Trend:** A line chart for week-over-week revenue and YTD growth.
    5.  **Monthly Revenue by Customer Segment:** A stacked bar chart showing the monthly trend per segment.

## 4. Architectural Rules
*   **Environment:** We're using Databricks Free Edition with PySpark, Delta Lake, and Databricks SQL.
*   **Storage:** DBFS (`/FileStore/tables/` for CSVs, `/FileStore/delta/` for Delta tables).
*   **Orchestration:** Each layer needs an orchestrator script (`ingest_all.py`, `create_silver_tables.py`, `create_gold_tables.py`) to run the individual scripts in the right order.
*   **Error Handling:** The orchestrators need a "fail-and-continue" setup with try/except blocks. If one script fails, it shouldn't block the rest of the layer. But it still needs to throw a `sys.exit(1)` at the end if there was a partial failure, so job monitors can catch it.
*   **Code Quality:** Keep the code commented and modular (like using `importlib` for running scripts in sequence). Basically, make it look production-ready.

## 5. Planted Data Quality Issues
These are the exact issues I planted to test the pipeline:
*   **Customers:** 50 NULL emails, 10 duplicate `customer_ids`.
*   **Orders:** 100 NULL `customer_ids`, 200 NULL `product_ids`, 50 orphan `customer_ids`, 30 orphan `product_ids`, 20 duplicate `order_ids`.
*   **Products:** 500 clean rows (no intentional issues).

## 6. Acceptance Criteria

*   **Bronze Layer:** 
    *   Record counts and NULL/duplicate profiles in the Bronze Delta tables have to match the raw CSV data exactly.
    *   `ingestion_timestamp` and `source_file_name` are populated correctly.
*   **Silver Layer:**
    *   Final Silver tables must have the exact same number of rows as Bronze (again, no data deleted).
    *   The `quality_check_result` column needs to accurately catch all the planted issues with the right string format.
*   **Gold Layer:**
    *   Aggregations strictly use `PASS` rows.
    *   Monetary values are rounded to 2 decimal places.
    *   The table schemas match exactly what the dashboard SQL queries expect.
