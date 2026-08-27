# AI Prompt Diary — Dashboard

This file records the chat history and decisions made while designing the Databricks SQL dashboard queries and dashboard setup guide.

---

## Prompt 1: Generate dashboard_queries.sql

**PROMPT SENT:**
> [.cursorrules active] Generate src/dashboard/dashboard_queries.sql only as one file, pure SQL, no PySpark. these are databricks SQL queries for a business dashboard. gold delta tables will be registered as SQL tables before running these.
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

**AI RESPONSE:**
It generated the `dashboard_queries.sql` file. For Query 2, it used `CASE WHEN` to bucket customers, but cleverly prefixed the labels with `1.`, `2.`, etc., so the SQL output would sort correctly. For Query 4, it actually added a `ytd_revenue` running total using a window function without me asking. Query 5 properly joined `silver_orders` and `gold_revenue_by_customer` and grouped by `DATE_TRUNC('MONTH')`.

**WHAT I KEPT:**
- The numbered prefixes on the bucket labels in Query 2 was a smart save. Without them, Databricks SQL sorts alphabetically, so `$5,000` comes before `$500`. The prefix forces the correct order on the chart axis.
- The unprompted `ytd_revenue` running total in Query 4 was a great idea. It lets us plot the weekly bumps alongside the cumulative growth on the same chart.
- Going back to `silver_orders` for Query 5 was correct. The Gold tables don't have row-level dates anymore, so we had to hit Silver to group by month.
- Using `COUNT(DISTINCT o.order_id)` in Query 5 instead of just `COUNT(*)` prevents counting duplicate rows from the join.

**WHAT I TWEAKED:**
- Query 4 selected `year` instead of `order_year`. That breaks because `year` is a reserved SQL keyword and it didn't match the Gold table schema. Fixed it.
- In Query 5, the join was `FROM gold_revenue_by_customer c JOIN silver_orders o`. I flipped the order so `silver_orders` is the driving fact table. Makes more sense semantically since we're looking at revenue over time.

**WHAT I THREW OUT:**
- Query 3 sorted the pie chart by `total_revenue DESC`, but the goal was to show customer distribution. Changed it to `ORDER BY customer_count DESC`.

**VERDICT:**
Accepted after tweaking the column names and join order. 
---

## Prompt 2: Generate database/schema.sql

**PROMPT SENT:**
> [.cursorrules active] Generate database/schema.sql only as one file, reference CREATE TABLE statements for all 12 delta tables in the pipeline. bronze 3, silver 3, gold 6.
> 
> note at top: these are reference schema definitions only. actual tables created by pyspark scripts.
> 
> for each table include: comment block above with layer, purpose, reads from, written by. all columns with correct types. inline comments on key columns (NULLs preserved, PK, FK, planted issue counts). the silver quality_check_result column with the PASS / FAIL format documented.
> 
> use CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION '...' format — makes these usable as registration statements too.

**AI RESPONSE:**
It generated `database/schema.sql` covering all 12 tables. It added inline comments documenting the planted data issues on the Bronze tables (e.g., `-- NULLs preserved (50 planted)`). For the Silver tables, it listed which quality checks were run in the header block. For Gold tables, it cross-referenced which dashboard query uses them. It used the `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION` syntax so the file acts as both documentation and executable setup code.

**WHAT I KEPT:**
- Using the `USING DELTA LOCATION` syntax was perfect. It means I can just copy-paste these into Databricks SQL to register the tables for the dashboard.
- The inline comments tracking exactly where the dirty data is planted (`-- 50 planted NULLs`) make the file super useful as a quick reference sheet.
- Listing the specific Silver quality checks in the header block (`-- Checks run: completeness, uniqueness...`) helps track lineage without having to read the python code.
- Using `DOUBLE` for `revenue_percent_rank` instead of `DECIMAL`. `PERCENT_RANK()` outputs a float, so `DECIMAL` would lose precision.

**WHAT I TWEAKED:**
- It set `total_orders` as a `BIGINT`. Technically true (Spark's `COUNT` returns a big int), but for 100k rows, `INT` is fine. I left it as `BIGINT` anyway so it wouldn't conflict with Spark's underlying schema.
- Fixed `year` to `order_year` in the `gold_weekly_trends` definition to match the fix I made in the python script.
- Standardized the header formats so they all looked like `-- Bronze | Raw customer data` instead of mixing styles.

**WHAT I THREW OUT:**
- It tried to document all 10 intermediate Silver tables (like `silver_customers_completeness`). I ripped those out. We only care about the final 12 tables. The intermediate ones are just pipeline guts, not part of the schema contract.
- It tried to add `NOT NULL` constraints to the primary keys in the schema. Databricks Delta doesn't actually enforce those at write time unless configured separately, and the Bronze layer intentionally has NULLs anyway. Removed the constraints and just used comments to explain them.

**VERDICT:**
Accepted after ripping out the intermediate tables and the fake `NOT NULL` constraints. The dual-purpose setup (documentation + runnable code) worked perfectly.

---

## Prompt 3: Generate DASHBOARD_GUIDE.md

**PROMPT SENT:**
> [.cursorrules active] Generate src/dashboard/DASHBOARD_GUIDE.md , only as one file, step by step guide to set up the databricks SQL dashboard manually.
> 
> step 1: register gold delta tables as SQL tables. include the exact CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION statements for all 5 tables needed (4 gold + silver_orders for query 5).
> step 2: how to create a new dashboard in databricks SQL.
> step 3: for each of the 5 queries — which visualization type, what goes on X axis, Y axis, color grouping, what the chart should look like, any tips.
> step 4: how to arrange tiles on the dashboard — include a layout diagram.
> step 5: add a dashboard title text block.
> troubleshooting section at the end — common failure modes specific to community edition.

**AI RESPONSE:**
It generated the guide. Step 1 had the SQL statements to register the tables. Step 3 used markdown tables (Setting | Value) to explicitly define how to configure each chart. Step 4 included a little ASCII art box diagram for the dashboard layout. The troubleshooting section covered basic Community Edition failures.

**WHAT I KEPT:**
- Using markdown tables (`Setting | Value`) to explain the chart config was great. Way better than trying to describe UI dropdowns in paragraphs.
- The ASCII layout diagram was super helpful. Saying "arrange the tiles" is useless; giving a visual map makes it idiot-proof.
- Having "Table not found" as the first troubleshooting step. Databricks CE drops the metastore when the cluster stops, so this is guaranteed to happen to anyone who runs the code one day and checks the dashboard the next.
- Explicitly including `silver_orders` in the Step 1 registration block, since Query 5 needs it. 

**WHAT I TWEAKED:**
- The config for Query 4 (weekly trends) just said `X axis: week_number`. Without grouping by year, multi-year data would overlay on top of itself. Added `Group by: order_year` to the config table so the chart splits the lines properly.
- For the Query 2 sorting issue, the troubleshooting section just said "Databricks sorts alphabetically". That's not helpful enough. I updated it to explicitly mention the numbered prefix trick (`1.`, `2.`) so the user knows *why* the order looks weird if they mess with the query.

**WHAT I THREW OUT:**
- Nothing

**VERDICT:**
Accepted 
---

## Alternative Easy Approach: Databricks Genie

While the manual SQL setup documented above is useful for learning, we also utilized **Databricks Genie** as an alternative, zero-code approach to automatically generate the dashboard directly from our Gold tables. Because the Gold tables were already structured as clean, business-ready aggregations, Genie was able to parse them instantly and build the visualisations without any manual query writing or UI configuration on our end.

**PROMPT SENT TO GENIE:**
> I need to build an Executive Medallion Dashboard using the tables in the workspace.default schema. Please analyze my Gold tables and automatically generate a dashboard with the following 4 specific visualizations:
>
> Total Revenue KPI (Counter/Scorecard): Sum the total_revenue column from the workspace.default.gold_customer_segmentation table to show our overall business revenue.
>
> Top 5 Products by Revenue (Bar Chart): Query the workspace.default.gold_sales_by_product table. Put product_name on the X-axis and total_revenue on the Y-axis. Sort it descending by revenue and limit the results to the top 5 products.
> 
> Weekly Revenue Trend (Line Chart): Query the workspace.default.gold_weekly_trends table. Put week_number on the X-axis and weekly_revenue on the Y-axis to show our performance over time.
>
> Customer Segmentation Breakdown (Pie/Donut Chart): Query the workspace.default.gold_customer_segmentation table. Group the chart by segment_type and use customer_count as the value/angle to show the percentage split of our customer base.
> 
> Please generate these charts and assemble them into a clean dashboard.

**OUTCOME:**
Genie successfully interpreted the schema of the `workspace.default` Gold tables and automatically generated the 4 requested charts:
- The KPI Counter accurately aggregated the total revenue.
- The Bar Chart correctly ordered the top 5 products.
- The Line Chart mapped out the 52-week trend seamlessly.
- The Pie/Donut Chart successfully showed the split (e.g., Repeat customers comprising 78%).

This approach demonstrated the true power of the Medallion architecture: by doing the heavy lifting in PySpark during the Bronze/Silver/Gold phases, the final presentation layer becomes incredibly simple to automate using modern AI assistants like Genie.
