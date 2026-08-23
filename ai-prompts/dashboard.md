# AI Prompt Diary — Dashboard

This file records all AI-assisted interactions related to the Databricks SQL dashboard queries and dashboard setup guide.

---

## Prompt 1: Generate dashboard_queries.sql

**PROMPT SENT:**
> [.cursorrules active] generate src/dashboard/dashboard_queries.sql — pure SQL, no PySpark. these are databricks SQL queries for a business dashboard. gold delta tables will be registered as SQL tables before running these.
>
> write 5 queries. format each with a header comment block: Query N, title, dashboard tile type, business question it answers.
>
> Query 1 — Top 10 products by revenue: SELECT product_name, category, total_revenue, total_orders, avg_order_value FROM gold_sales_by_product ORDER BY total_revenue DESC LIMIT 10. bar chart.
>
> Query 2 — Customer revenue distribution: bucket customers into spend ranges: 0-500, 500-1000, 1000-2500, 2500-5000, 5000-10000, 10000+. show bucket_label, customer_count, avg_revenue_in_bucket. read from gold_revenue_by_customer. histogram/bar chart.
>
> Query 3 — Customer segmentation breakdown: SELECT segment_type, customer_count, total_revenue, avg_revenue FROM gold_customer_segmentation ORDER BY customer_count DESC. pie chart.
>
> Query 4 — Weekly revenue trend: SELECT week_number, order_year, weekly_revenue, weekly_orders FROM gold_weekly_trends ORDER BY order_year ASC, week_number ASC. line chart.
>
> Query 5 — Revenue by customer segment over time: monthly revenue per customer_segment. needs order-level date granularity so join silver_orders (PASS + Completed only) with gold_revenue_by_customer for the segment label. group by month and customer_segment. stacked bar chart.
>
> add a divider line between queries.

**AI RESPONSE SUMMARY:**
Generated `dashboard_queries.sql` with all five queries and structured header comment blocks. Query 1 was exactly as specified. Query 2 used `CASE WHEN` buckets with string labels prefixed `1.`, `2.`, etc. (`'1. $0 – $500'`, `'2. $500 – $1,000'`) to force correct sort order in Databricks SQL. Query 3 was as specified. Query 4 added an unprompted `ytd_revenue` running cumulative column using `SUM(weekly_revenue) OVER (PARTITION BY order_year ORDER BY week_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`. Query 5 joined `silver_orders` to `gold_revenue_by_customer` on `customer_id`, filtered PASS + Completed, grouped by `DATE_TRUNC('MONTH', o.order_date)` and `c.customer_segment`.

**WHAT I ACCEPTED:**
- Prefixing bucket labels with `1.`, `2.`... in Query 2 was a non-obvious but correct fix for Databricks SQL's alphabetical string sort. Without the prefix, `$5,000–$10,000` sorts before `$500–$1,000` alphabetically because `5` > `5` but `$5` < `$500` in ASCII comparison — the bucket bar chart would be completely out of order. The numbered prefix forces the correct sort without needing a numeric sort key column.
- The unprompted `ytd_revenue` window function addition in Query 4 was a good call. It lets you overlay both weekly fluctuation and cumulative growth on the same line chart widget without a second query or a separate tile. `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is the correct frame spec for a running total — `RANGE BETWEEN` would give wrong results on ties. Kept it.
- Query 5 correctly joining `silver_orders` instead of a Gold table. The Gold tables lose individual `order_date` — `gold_revenue_by_customer` only has aggregated totals with no date dimension. Monthly granularity requires row-level dates, so `silver_orders` was the right source. The filter `WHERE quality_check_result = 'PASS' AND order_status = 'Completed'` mirrors the same filter logic used in the Gold trends script.
- `COUNT(DISTINCT o.order_id)` in Query 5 instead of plain `COUNT(*)` — after the join, a single order could theoretically appear multiple times if the join produced duplicates. The DISTINCT guards against inflated counts.

**WHAT I CHANGED:**
- Query 4 originally selected `week_number` and `year` as column names. The Gold table uses `order_year` (not `year`) — `year` is also a reserved keyword in Spark SQL which could cause parsing issues. Fixed the column reference to `order_year` to match the actual schema from `gold_weekly_trends`.
- The Query 5 join was initially `FROM gold_revenue_by_customer c JOIN silver_orders o` — gold table as the driving table. This means any customer who had completed orders but all of them failed quality checks would still appear in the Gold table (they might have passed quality checks for other dimensions) but would return 0 revenue rows after the Silver filter. Flipped to `FROM silver_orders o JOIN gold_revenue_by_customer c` so Silver orders are the fact table and the join brings in the segment label. Makes more semantic sense for a monthly revenue query.

**WHAT I REJECTED:**
- The first version of Query 2 used unlabelled numeric buckets (`CASE WHEN total_revenue < 500 THEN 1 WHEN ... THEN 2`) — just integers, no labels. A bar chart with X axis values `1, 2, 3, 4, 5, 6` is meaningless to a business user. Changed to descriptive string labels. The numbered prefix approach emerged after I pointed out the sort order problem with plain string labels.
- Query 3 initially had `ORDER BY total_revenue DESC` instead of `ORDER BY customer_count DESC`. For a pie chart the sort order affects which segment gets the "default" first slice, but more importantly the business question on the header said "proportion of customer base" — so sorting by customer count makes the pie easier to read for population distribution, not revenue ranking. Changed to `customer_count DESC`.

**FINAL DECISION:**
Accepted after fixing the `order_year` column reference in Query 4 and swapping the Query 5 join direction. The `ytd_revenue` running total was an unprompted bonus that made Query 4 significantly more useful for the line chart tile.

---

## Prompt 2: Generate database/schema.sql

**PROMPT SENT:**
> [.cursorrules active] generate database/schema.sql — reference CREATE TABLE statements for all 12 delta tables in the pipeline. bronze 3, silver 3, gold 6.
>
> note at top: these are reference schema definitions only. actual tables created by pyspark scripts.
>
> for each table include: comment block above with layer, purpose, reads from, written by. all columns with correct types. inline comments on key columns (NULLs preserved, PK, FK, planted issue counts). the silver quality_check_result column with the PASS / FAIL format documented.
>
> use CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION '...' format — makes these usable as registration statements too, not just documentation.

**AI RESPONSE SUMMARY:**
Generated `database/schema.sql` covering all 12 tables. Bronze tables documented the planted quality issues inline on column comments (e.g., `-- NULLs preserved (50 planted)`, `-- duplicates preserved, 20 planted`). Silver tables each listed which quality checks were run in the comment block (`-- Checks run: completeness, uniqueness, type_validation, business_logic`). Gold tables cross-referenced their dashboard query number in the comment block (`-- Dashboard: Query 1 — Top 10 products by revenue`). The file header explained the `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION` pattern and noted that these statements can also serve as table registration commands for the dashboard.

**WHAT I ACCEPTED:**
- `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION '...'` format was exactly right — it means `schema.sql` is dual-purpose: documentation of the schema contract AND runnable table registration statements for the dashboard setup. The same statements appear verbatim in `DASHBOARD_GUIDE.md` Step 1. Generating them here first and copying to the guide saved time.
- Inline column comments with planted issue counts on Bronze tables (e.g., `-- FK → customers (NULLs preserved, 100 planted)`) made the schema file genuinely useful as a reference — someone reading it immediately understands what quality issues exist and where.
- `-- Checks run:` per Silver table was a useful addition. The final `silver_customers`, `silver_orders`, `silver_products` tables each went through a different number of quality checks — documenting which checks fed each table makes the lineage traceable without running the code.
- `revenue_percent_rank DOUBLE` on `gold_customer_segment_detail` was correct — `PERCENT_RANK()` returns a float between 0 and 1, not a `DECIMAL`. Using `DECIMAL` here would lose precision unnecessarily.
- The `-- Dashboard: Query N` cross-reference on Gold tables links the schema directly to the query that reads from each table. Useful for navigating between files.

**WHAT I CHANGED:**
- The initial schema had `total_orders BIGINT` on Gold tables. I questioned this — the Gold aggregations count orders using `COUNT(order_id)` which in Spark SQL returns `BIGINT`. So `BIGINT` is technically correct, but for 100,000 total orders an `INT` would be more than sufficient. Left it as `BIGINT` because that's what Spark actually writes and I didn't want the schema to conflict with what's in the Delta table.
- The first version used `year INT` as the column name in `gold_weekly_trends`. Changed to `order_year INT` to match the actual column name in the script — I'd just fixed the same mismatch in Query 4 of `dashboard_queries.sql`. Schema and query had to be consistent.
- The Bronze table comment blocks initially said `-- Layer: Bronze Raw Ingest` at the top. Changed to the more concise `-- Bronze | Raw customer data` format to match the style used for Silver and Gold tables — consistent visual scanning across all 12 tables.

**WHAT I REJECTED:**
- The first attempt included intermediate Silver quality-check tables in the schema (`silver_customers_completeness`, `silver_customers_uniqueness`, etc. — all 10 intermediate tables). I removed all of them. The brief asked for 12 tables: 3 Bronze + 3 Silver (final) + 6 Gold. The intermediate check tables are internal pipeline artefacts, not part of the schema contract. The schema file documents the interfaces between layers, not the internal workings of the Silver transformation layer.
- It added `NOT NULL` constraints on PK columns like `customer_id`, `order_id`. Databricks Delta Lake doesn't enforce `NOT NULL` constraints during writes unless you configure Delta table constraints separately — the `NOT NULL` annotation in a `CREATE TABLE` is ignored at write time in Spark SQL. Including it would be misleading since the Bronze layer explicitly preserves NULL PKs. Removed them and documented the intentional NULLs in inline comments instead.

**FINAL DECISION:**
Accepted after removing the intermediate Silver tables, fixing `year` → `order_year`, and removing the misleading `NOT NULL` constraints. The dual-purpose nature of the file (documentation + runnable registration statements) was exactly the right design.

---

## Prompt 3: Generate DASHBOARD_GUIDE.md

**PROMPT SENT:**
> [.cursorrules active] generate src/dashboard/DASHBOARD_GUIDE.md — step by step guide to set up the databricks SQL dashboard manually.
>
> step 1: register gold delta tables as SQL tables. include the exact CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION statements for all 5 tables needed (4 gold + silver_orders for query 5).
> step 2: how to create a new dashboard in databricks SQL.
> step 3: for each of the 5 queries — which visualization type, what goes on X axis, Y axis, color grouping, what the chart should look like, any tips.
> step 4: how to arrange tiles on the dashboard — include a layout diagram.
> step 5: add a dashboard title text block.
> troubleshooting section at the end — common failure modes specific to community edition.

**AI RESPONSE SUMMARY:**
Generated `DASHBOARD_GUIDE.md` with five numbered steps. Step 1 included `CREATE TABLE IF NOT EXISTS` statements for all four Gold tables plus `silver_orders`, with a `SHOW TABLES` verification step. Step 3 used a markdown table per query (Setting / Value columns) covering visualization type, X axis, Y axis, color grouping, and title. Step 4 included an ASCII box-drawing tile layout diagram. Step 5 provided a text tile template with the dashboard title and data range. Troubleshooting table covered five common failure modes with cause and fix columns.

**WHAT I ACCEPTED:**
- Per-query markdown tables with `Setting | Value` rows were exactly the right format for visualization configuration instructions. Prose paragraphs describing how to configure a chart are ambiguous — a table with explicit X axis / Y axis / Color by rows is directly actionable. Each row is a UI field the user sets.
- The ASCII tile layout diagram was a useful addition that I hadn't specified explicitly. Without it, "arrange the tiles" is vague — the diagram gives a concrete target layout. The two-column grid with the line chart spanning full width in the middle reflects how I'd actually lay out the dashboard.
- Troubleshooting table with "Table not found" as the first row was correct — it's by far the most common failure in Databricks Community Edition. The metastore doesn't persist registered tables across cluster restarts, so anyone who ran the pipeline yesterday and comes back today will hit this immediately.
- The `silver_orders` registration statement included in Step 1 (not just Gold tables) was important — Query 5 joins Silver, so forgetting to register `silver_orders` would cause that query to fail with a confusing `Table not found: silver_orders` error that's not obviously related to Gold setup.

**WHAT I CHANGED:**
- The initial Query 4 visualization table had `X axis: week_number` without mentioning `order_year`. For multi-year data, using only `week_number` on the X axis would superimpose all years on top of each other — week 1 of 2021 and week 1 of 2022 would merge into one data point. Added `Group by: order_year` to the table and noted that this splits the line into separate series per year.
- The troubleshooting row for "X axis order is wrong" initially explained the issue as "Databricks sorts alphabetically". Changed the explanation to be more specific: the numbered prefix (`1.`, `2.`...) in Query 2's bucket labels is what prevents the sort problem, and the fix column tells the user to confirm `ORDER BY bucket_label ASC` is present. Generic advice ("sort alphabetically") without explaining the prefix mechanism wouldn't help.

**WHAT I REJECTED:**
- The first version included a Step 6: "Refresh Schedule" explaining how to set up automatic dashboard refresh in Databricks. Databricks Community Edition doesn't support scheduled dashboard refresh — it's a Pro/Enterprise feature. Including instructions for a feature that doesn't exist in the target environment would cause confusion. Removed it entirely.
- It suggested adding filters to Query 1 (a dropdown for `category`) to make the bar chart interactive. While useful in a real deployment, I removed the filter instruction because Databricks SQL Community Edition's dashboard filter support is limited and the filter widget behaviour varies by cluster setup. Better to keep the guide to what's guaranteed to work.

**FINAL DECISION:**
Accepted after adding `order_year` grouping to Query 4's visualization table, sharpening the troubleshooting row for Query 2 sort order, and removing the scheduled refresh section. The guide is complete enough for someone to reproduce the dashboard from scratch in a fresh Community Edition workspace.
