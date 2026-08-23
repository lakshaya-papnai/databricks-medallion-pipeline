-- ============================================================
-- Databricks SQL Dashboard Queries
-- Project: E-Commerce Medallion Architecture Pipeline
-- Source:  Gold Delta tables registered as SQL tables
-- Note:    Run these queries in Databricks SQL Editor.
--          Register Gold tables first:
--            CREATE TABLE gold_sales_by_product
--              USING DELTA LOCATION '/FileStore/delta/gold/gold_sales_by_product';
--            (repeat for each Gold table)
-- ============================================================


-- ============================================================
-- Query 1: Top 10 Products by Revenue
-- Dashboard tile : Bar Chart
-- Business question: Which products are generating the most
--                    revenue, and how do they compare across
--                    categories? Helps the merchandising team
--                    prioritise stock and promotional spend.
-- ============================================================

SELECT
    product_name,
    category,
    total_revenue,
    total_orders,
    avg_order_value
FROM gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;


-- ============================================================
-- Query 2: Customer Revenue Distribution (Histogram)
-- Dashboard tile : Histogram / Bar Chart
-- Business question: How is revenue spread across the customer
--                    base? Are most customers low-spend or
--                    high-spend? Helps identify if revenue is
--                    concentrated in a small group of buyers.
-- ============================================================

SELECT
    -- Assign each customer to a labelled spend bucket
    CASE
        WHEN total_revenue <   500  THEN '1. $0 – $500'
        WHEN total_revenue <  1000  THEN '2. $500 – $1,000'
        WHEN total_revenue <  2500  THEN '3. $1,000 – $2,500'
        WHEN total_revenue <  5000  THEN '4. $2,500 – $5,000'
        WHEN total_revenue < 10000  THEN '5. $5,000 – $10,000'
        ELSE                             '6. $10,000+'
    END                                         AS bucket_label,
    COUNT(customer_id)                          AS customer_count,
    ROUND(AVG(total_revenue), 2)                AS avg_revenue_in_bucket
FROM gold_revenue_by_customer
GROUP BY bucket_label
ORDER BY bucket_label ASC;


-- ============================================================
-- Query 3: Customer Segmentation Breakdown (Pie Chart)
-- Dashboard tile : Pie / Donut Chart
-- Business question: What proportion of the customer base falls
--                    into each behavioural segment (High-Value,
--                    Repeat, One-Time, Inactive)? Drives
--                    targeted retention and re-engagement
--                    campaigns.
-- ============================================================

SELECT
    segment_type,
    customer_count,
    total_revenue,
    avg_revenue
FROM gold_customer_segmentation
ORDER BY customer_count DESC;


-- ============================================================
-- Query 4: Weekly Revenue Trend (Line Chart)
-- Dashboard tile : Line Chart
-- Business question: How is revenue trending week-over-week?
--                    Are there seasonal peaks or troughs that
--                    the business should plan for?
-- ============================================================

SELECT
    order_year,
    week_number,
    weekly_revenue,
    weekly_orders,
    -- Compute a running cumulative revenue for the year
    -- so the line chart can show both weekly and YTD views
    ROUND(
        SUM(weekly_revenue) OVER (
            PARTITION BY order_year
            ORDER BY week_number ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    )                                           AS ytd_revenue
FROM gold_weekly_trends
ORDER BY order_year ASC, week_number ASC;


-- ============================================================
-- Query 5: Monthly Revenue by Customer Segment (Stacked Bar)
-- Dashboard tile : Stacked Bar Chart
-- Business question: How does revenue from each customer
--                    segment (Premium / Standard / Basic) trend
--                    month-by-month? Identifies which segment
--                    is growing, flat, or declining over time.
-- Source tables   : gold_revenue_by_customer (segment label)
--                   silver_orders (order-level dates + amounts,
--                   PASS rows and Completed status only)
-- ============================================================

SELECT
    DATE_TRUNC('MONTH', o.order_date)           AS order_month,
    c.customer_segment,
    COUNT(DISTINCT o.order_id)                  AS monthly_orders,
    ROUND(SUM(o.total_amount), 2)               AS monthly_revenue
FROM silver_orders         o
JOIN gold_revenue_by_customer c
    ON o.customer_id = c.customer_id
WHERE o.quality_check_result = 'PASS'   -- Silver PASS rows only
  AND o.order_status        = 'Completed'
GROUP BY
    DATE_TRUNC('MONTH', o.order_date),
    c.customer_segment
ORDER BY
    order_month       ASC,
    customer_segment  ASC;
