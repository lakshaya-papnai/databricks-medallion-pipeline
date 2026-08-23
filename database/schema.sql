-- ============================================================
-- NOTE: These are reference schema definitions only.
-- Actual Delta tables are created by PySpark scripts in src/
-- This file serves as documentation and schema contract for
-- the entire Databricks Medallion Architecture pipeline.
--
-- Pipeline layers:
--   Bronze → /FileStore/delta/bronze/
--   Silver → /FileStore/delta/silver/
--   Gold   → /FileStore/delta/gold/
--
-- To register a Delta table in Databricks SQL:
--   CREATE TABLE <table_name>
--     USING DELTA
--     LOCATION '<delta_path>';
-- ============================================================


-- ============================================================
-- BRONZE LAYER
-- Purpose : Raw ingest — no cleaning, no transformations.
--           Every source row is preserved exactly as-is.
--           Two metadata columns added to every Bronze table:
--             ingestion_timestamp — when the row was loaded
--             source_file_name    — which CSV file it came from
-- ============================================================

-- ------------------------------------------------------------
-- Bronze | Raw customer data
-- Reads from : /FileStore/tables/customers.csv
-- Written by : src/bronze/01_ingest_customers.py
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_customers (
    customer_id          INT,             -- PK (duplicates preserved as-is)
    customer_name        STRING,
    email                STRING,          -- NULLs preserved (50 planted)
    country              STRING,
    signup_date          DATE,
    customer_segment     STRING,          -- Premium / Standard / Basic
    lifetime_value       DECIMAL(10, 2),
    ingestion_timestamp  TIMESTAMP,       -- Metadata: load time
    source_file_name     STRING           -- Metadata: 'customers.csv'
)
USING DELTA
LOCATION '/FileStore/delta/bronze/bronze_customers';


-- ------------------------------------------------------------
-- Bronze | Raw order transaction data
-- Reads from : /FileStore/tables/orders.csv
-- Written by : src/bronze/02_ingest_orders.py
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id             INT,             -- PK (duplicates preserved, 20 planted)
    customer_id          INT,             -- FK → customers (NULLs preserved, 100 planted)
    order_date           DATE,
    product_id           INT,             -- FK → products (NULLs preserved, 200 planted)
    quantity             INT,
    unit_price           DECIMAL(10, 2),
    total_amount         DECIMAL(10, 2),
    order_status         STRING,          -- Pending / Completed / Cancelled
    payment_date         DATE,            -- Nullable: NULL for Pending/Cancelled orders
    ingestion_timestamp  TIMESTAMP,       -- Metadata: load time
    source_file_name     STRING           -- Metadata: 'orders.csv'
)
USING DELTA
LOCATION '/FileStore/delta/bronze/bronze_orders';


-- ------------------------------------------------------------
-- Bronze | Raw product catalogue data
-- Reads from : /FileStore/tables/products.csv
-- Written by : src/bronze/03_ingest_products.py
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_products (
    product_id           INT,             -- PK
    product_name         STRING,
    category             STRING,
    price                DECIMAL(10, 2),  -- Selling price
    cost                 DECIMAL(10, 2),  -- Cost of goods; always < price
    stock_quantity       INT,
    reorder_level        INT,             -- Minimum stock before reorder
    ingestion_timestamp  TIMESTAMP,       -- Metadata: load time
    source_file_name     STRING           -- Metadata: 'products.csv'
)
USING DELTA
LOCATION '/FileStore/delta/bronze/bronze_products';


-- ============================================================
-- SILVER LAYER
-- Purpose : Quality-checked data. Bad rows are NEVER deleted —
--           they are flagged with quality_check_result column.
--           quality_check_result = 'PASS' → row passed all checks
--           quality_check_result = 'FAIL - <reason>' → flagged
-- Reads from : Bronze Delta tables only (never raw CSVs)
-- ============================================================

-- ------------------------------------------------------------
-- Silver | Quality-checked customer data
-- Reads from  : bronze_customers
-- Written by  : src/silver/create_silver_tables.py
-- Checks run  : completeness, uniqueness, type_validation,
--               business_logic
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id           INT,
    customer_name         STRING,
    email                 STRING,
    country               STRING,
    signup_date           DATE,
    customer_segment      STRING,
    lifetime_value        DECIMAL(10, 2),
    ingestion_timestamp   TIMESTAMP,
    source_file_name      STRING,
    quality_check_result  STRING          -- 'PASS' or 'FAIL - <reason(s)>'
)
USING DELTA
LOCATION '/FileStore/delta/silver/silver_customers';


-- ------------------------------------------------------------
-- Silver | Quality-checked order transaction data
-- Reads from  : bronze_orders
-- Written by  : src/silver/create_silver_tables.py
-- Checks run  : completeness, uniqueness, referential_integrity,
--               type_validation, business_logic
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id              INT,
    customer_id           INT,
    order_date            DATE,
    product_id            INT,
    quantity              INT,
    unit_price            DECIMAL(10, 2),
    total_amount          DECIMAL(10, 2),
    order_status          STRING,
    payment_date          DATE,
    ingestion_timestamp   TIMESTAMP,
    source_file_name      STRING,
    quality_check_result  STRING          -- 'PASS' or 'FAIL - <reason(s)>'
)
USING DELTA
LOCATION '/FileStore/delta/silver/silver_orders';


-- ------------------------------------------------------------
-- Silver | Quality-checked product catalogue data
-- Reads from  : bronze_products
-- Written by  : src/silver/create_silver_tables.py
-- Checks run  : type_validation (price/cost/stock ranges)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_products (
    product_id            INT,
    product_name          STRING,
    category              STRING,
    price                 DECIMAL(10, 2),
    cost                  DECIMAL(10, 2),
    stock_quantity        INT,
    reorder_level         INT,
    ingestion_timestamp   TIMESTAMP,
    source_file_name      STRING,
    quality_check_result  STRING          -- 'PASS' or 'FAIL - <reason(s)>'
)
USING DELTA
LOCATION '/FileStore/delta/silver/silver_products';


-- ============================================================
-- GOLD LAYER
-- Purpose : Business-ready aggregations for analytics and BI.
--           All Gold tables use PASS rows from Silver only.
--           All monetary values rounded to 2 decimal places.
-- Reads from : Silver Delta tables only
-- ============================================================

-- ------------------------------------------------------------
-- Gold | Aggregated sales metrics per product
-- Reads from  : silver_orders, silver_products (PASS only)
-- Written by  : src/gold/01_sales_by_product.py
-- Dashboard   : Query 1 — Top 10 products by revenue (bar chart)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_sales_by_product (
    product_id       INT,
    product_name     STRING,
    category         STRING,
    total_orders     BIGINT,              -- COUNT of orders for this product
    total_revenue    DECIMAL(10, 2),      -- SUM of total_amount
    avg_order_value  DECIMAL(10, 2)       -- AVG of total_amount per order
)
USING DELTA
LOCATION '/FileStore/delta/gold/gold_sales_by_product';


-- ------------------------------------------------------------
-- Gold | Aggregated revenue metrics per customer
-- Reads from  : silver_orders, silver_customers (PASS only)
-- Written by  : src/gold/02_revenue_by_customer.py
-- Dashboard   : Query 2 — Revenue distribution histogram
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_revenue_by_customer (
    customer_id           INT,
    customer_name         STRING,
    customer_segment      STRING,         -- Premium / Standard / Basic (from source)
    total_orders          BIGINT,
    total_revenue         DECIMAL(10, 2),
    avg_order_value       DECIMAL(10, 2),
    lifetime_value_actual DECIMAL(10, 2)  -- Carried through from silver_customers
)
USING DELTA
LOCATION '/FileStore/delta/gold/gold_revenue_by_customer';


-- ------------------------------------------------------------
-- Gold | Daily revenue and order volume
-- Reads from  : silver_orders (PASS + Completed only)
-- Written by  : src/gold/03_daily_weekly_trends.py
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_daily_trends (
    order_date             DATE,
    daily_orders           BIGINT,
    daily_revenue          DECIMAL(10, 2),
    daily_avg_order_value  DECIMAL(10, 2)
)
USING DELTA
LOCATION '/FileStore/delta/gold/gold_daily_trends';


-- ------------------------------------------------------------
-- Gold | Weekly revenue and order volume
-- Reads from  : silver_orders (PASS + Completed only)
-- Written by  : src/gold/03_daily_weekly_trends.py
-- Dashboard   : Query 4 — Weekly revenue trend (line chart)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_weekly_trends (
    order_year              INT,
    week_number             INT,          -- ISO week number (1–53)
    weekly_orders           BIGINT,
    weekly_revenue          DECIMAL(10, 2),
    weekly_avg_order_value  DECIMAL(10, 2)
)
USING DELTA
LOCATION '/FileStore/delta/gold/gold_weekly_trends';


-- ------------------------------------------------------------
-- Gold | Summary aggregation by behavioural segment type
-- Reads from  : silver_orders, silver_customers (PASS only)
-- Written by  : src/gold/04_customer_segmentation.py
-- Dashboard   : Query 3 — Segmentation pie chart
-- Segments    : High-Value (top 20% revenue), Repeat (2+ orders),
--               One-Time (1 order), Inactive (0 completed orders)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_customer_segmentation (
    segment_type    STRING,               -- High-Value / Repeat / One-Time / Inactive
    customer_count  BIGINT,
    avg_revenue     DECIMAL(10, 2),
    total_revenue   DECIMAL(10, 2)
)
USING DELTA
LOCATION '/FileStore/delta/gold/gold_customer_segmentation';


-- ------------------------------------------------------------
-- Gold | Customer-level detail with assigned segment
-- Reads from  : silver_orders, silver_customers (PASS only)
-- Written by  : src/gold/04_customer_segmentation.py
-- Purpose     : Supports dashboard drill-down from segment → customer
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_customer_segment_detail (
    customer_id              INT,
    customer_name            STRING,
    customer_segment         STRING,      -- Original segment from source (Premium/Standard/Basic)
    total_completed_orders   BIGINT,
    total_revenue            DECIMAL(10, 2),
    revenue_percent_rank     DOUBLE,      -- PERCENT_RANK() result (0.0 = top earner)
    segment_type             STRING       -- Assigned behavioural segment (High-Value/Repeat/etc.)
)
USING DELTA
LOCATION '/FileStore/delta/gold/gold_customer_segment_detail';
