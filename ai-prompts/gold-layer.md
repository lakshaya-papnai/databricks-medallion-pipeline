# AI Prompt Diary — Gold Layer

This file records the chat history and decisions made while designing the Gold layer aggregation scripts.

---

## Prompt 1: Generate 01_sales_by_product.py

**PROMPT SENT:**
> [.cursorrules active] Starting gold layer. Generate src/gold/01_sales_by_product.py only as one file .
> 
> reads from silver_orders and silver_products. gold rule: only use rows where quality_check_result = 'PASS'. never read from bronze or CSVs.
> 
> join orders to products on product_id. group by product_id, product_name, category. calculate: total_orders (COUNT order_id), total_revenue (SUM total_amount, rounded 2dp), avg_order_value (AVG total_amount, rounded 2dp). order by total_revenue desc.
> 
> write to /FileStore/delta/gold/gold_sales_by_product, mode overwrite.
> 
> validation after write: total products with sales, top 5 products by revenue, revenue by category (groupby category -> count products, sum revenue).
> 
> use spark.sql() with temp views for the aggregation,  I want it readable as SQL not PySpark dataframe chains. add comments. keep .py extension not .sql — .sql files can't write delta tables.

**AI RESPONSE:**
It wrote the script using the standard 7-section structure. It smartly filtered for `PASS` rows *before* calling `createOrReplaceTempView()`, so the temp view only had clean data. The SQL query used `ROUND(SUM(...), 2)` inline. After writing, it re-read the Delta table and registered a second temp view just to run the validation queries against the final written data.

**WHAT I KEPT:**
- Filtering PASS rows before registering the temp view was a great move. It keeps the SQL query clean and focused on business logic instead of data quality plumbing.
- Registering a second temp view for the validation was correct. Proves the data actually landed in Delta correctly.
- The category breakdown aggregated `SUM(total_revenue)` from the Gold table correctly (summing the already-aggregated product revenue, not raw order amounts).

**WHAT I TWEAKED:**
- The SQL used an `INNER JOIN`. At first I thought this was a mistake because it drops orders with a NULL `product_id`. But since we already filtered for PASS rows, those NULLs are gone anyway. INNER JOIN is actually correct here. I just added a comment explaining *why* it's correct so nobody tries to "fix" it to a LEFT JOIN later.
- The validation just called `show(5)`, which truncated long product names. I added `truncate=False` to all the show() calls so the logs are actually readable.

**WHAT I THREW OUT:**
- The first draft put the `WHERE quality_check_result = 'PASS'` filter inside the SQL query instead of the dataframe. Functionally the same, but doing it in the dataframe first means a smaller temp view and cleaner SQL. I made it move the filter out.
- It threw in a `HAVING COUNT(o.order_id) > 0` clause to exclude products with no sales. But products with no sales wouldn't show up in an INNER JOIN anyway. It was redundant, so I deleted it.

**VERDICT:**
Accepted after tweaking the filter placement and removing the HAVING clause. This `spark.sql()` temp view approach became the standard for all the Gold scripts.

---

## Prompt 2: Generate 02_revenue_by_customer.py

**PROMPT SENT:**
> [.cursorrules active] Generate src/gold/02_revenue_by_customer.py only. same structure and style as 01_sales_by_product.py.
> 
> reads from silver_orders (PASS only) and silver_customers (PASS only). join on customer_id. group by customer_id, customer_name, customer_segment.
> 
> metrics: total_orders, total_revenue (rounded 2dp), avg_order_value (rounded 2dp), lifetime_value_actual (carry through from silver_customers ,it's a customer attribute, not an aggregated value).
> 
> order by total_revenue desc. write to /FileStore/delta/gold/gold_revenue_by_customer.
> 
> validation: total customers with orders, top 5 by revenue, revenue breakdown by customer_segment (count, sum revenue, avg revenue per customer).

**AI RESPONSE:**
It wrote the script following the same pattern. To carry the `lifetime_value` through the `GROUP BY` without breaking the aggregation, it used `MAX(c.lifetime_value) AS lifetime_value_actual`. Validation used a second `spark.sql()` query for the segment breakdown.

**WHAT I KEPT:**
- Using `MAX()` to carry through a non-aggregated attribute is a classic SQL trick. Since `lifetime_value` is the same for every row of a given customer, `MAX()` just grabs it. It added a comment explaining the idiom which was nice: *"We use MAX() to de-aggregate it safely within the GROUP BY..."*
- The segment breakdown query hit all the dimensions I needed (count, sum, avg). Super useful for the dashboard.

**WHAT I TWEAKED:**
- The join was originally written as `FROM silver_orders o JOIN silver_customers c`. Logically fine, but the unit of analysis here is the *customer*. I flipped the join to `FROM silver_customers c JOIN silver_orders o` so it's framed correctly.
- Added `truncate=False` to the show() calls again.

**WHAT I THREW OUT:**
- The first pass included `c.country` and `c.signup_date` in the SELECT and GROUP BY. I didn't ask for those. Gold tables should be purpose-built for the dashboard, not just a dump of everything from Silver. Deleted them.
- It also tried to compute `lifetime_value_actual` using a weird subquery that re-read `silver_customers`. Totally unnecessary when the `MAX()` trick works in one pass. Ripped that out.

**VERDICT:**
Accepted after leaning out the extra columns and the subquery. 

---

## Prompt 3: Generate 03_daily_weekly_trends.py

**PROMPT SENT:**
> [.cursorrules active] Generate src/gold/03_daily_weekly_trends.py only as one file.
> 
> reads from silver_orders only. two filters: quality_check_result = 'PASS' AND order_status = 'Completed'. only completed orders represent confirmed revenue for trend analysis.
> 
> daily trends: group by order_date -> daily_orders (COUNT), daily_revenue (SUM rounded 2dp), daily_avg_order_value (AVG rounded 2dp). order by order_date asc.
> 
> weekly trends: group by year + week number -> use YEAR() and WEEKOFYEAR() spark sql functions. columns: order_year, week_number, weekly_orders, weekly_revenue, weekly_avg_order_value. order by year asc then week asc.
> 
> write both as separate gold delta tables: gold_daily_trends and gold_weekly_trends. validation: date range of completed orders (min/max date, total days), total day-level rows, total week-level rows, top 5 highest revenue days.

**AI RESPONSE:**
It wrote the script, chaining the two dataframe filters (`.filter("quality_check_result = 'PASS'").filter("order_status = 'Completed'")`) before making the view `silver_orders_completed`. Used `YEAR()` and `WEEKOFYEAR()` for the weekly grouping, and wrote out two separate Gold tables.

**WHAT I KEPT:**
- Naming the temp view `silver_orders_completed` was a great touch. It makes it instantly obvious that this view is already pre-filtered.
- Writing two separate Gold tables instead of one combined monstrosity was the right call. Dashboards hate mixed-granularity tables.
- Grouping by `YEAR()` alongside `WEEKOFYEAR()` is critical so week 1 of 2023 doesn't get mashed together with week 1 of 2024.

**WHAT I TWEAKED:**
- The weekly ordering was just `ORDER BY week_number ASC`. In a multi-year dataset, that sorts all "week 1s" together, then all "week 2s". Terrible for a time series. I updated it to `ORDER BY order_year ASC, week_number ASC`.
- The top-5 revenue days validation just used PySpark `.orderBy()` on the dataframe in memory. I swapped it to a `spark.sql()` query on the Gold temp view to keep the validation pattern consistent.

**WHAT I THREW OUT:**
-Nothing

**VERDICT:**
Accepted.
---

## Prompt 4: Generate 04_customer_segmentation.py

**PROMPT SENT:**
> [.cursorrules active] Generate src/gold/04_customer_segmentation.py only as one file , this is the most complex gold script, read carefully.
> 
> reads from silver_orders (PASS only) and silver_customers (PASS only).
> 
> step 1: compute per-customer metrics. LEFT JOIN customers to orders on customer_id. calc total_completed_orders (COUNT of Completed orders only) and total_revenue (SUM total_amount for Completed orders only). customers with zero completed orders must be included, they become Inactive. use COALESCE for their revenue so they get 0 not NULL.
> 
> step 2: PERCENT_RANK() ordered by total_revenue DESC. rank 0.0 = highest earner.
> 
> step 3: CASE WHEN in this exact priority order: High-Value (percent_rank <= 0.20), Inactive (total_completed_orders = 0), Repeat (total_completed_orders >= 2), One-Time (total_completed_orders = 1). add ELSE 'Unknown' as a safety net.
> 
> step 4: aggregate by segment_type -> customer_count, avg_revenue, total_revenue.
> 
> Write customer-level detail to gold_customer_segment_detail FIRST. Re-read from delta, then aggregate summary and write to gold_customer_segmentation. validation: total customers segmented, count per segment_type, confirm 0 rows with NULL or Unknown segment.

**AI RESPONSE:**
It wrote the script with four clear steps (metrics -> rank -> assign -> aggregate). Used a `LEFT JOIN` from customers to orders. Used `PERCENT_RANK() OVER (ORDER BY total_revenue DESC)`. Applied the `CASE WHEN` logic with the `ELSE 'Unknown'` safety net. Wrote the detail table, then re-read it to build the summary.

**WHAT I KEPT:**
- The `LEFT JOIN` from customers to orders was critical. An inner join would drop customers with zero orders, making it impossible to flag them as 'Inactive'. 
- Doing the `COUNT` and `SUM` with a `CASE WHEN order_status = 'Completed'` inside the aggregate functions was a great way to filter completed orders without dropping the Inactive customers from the join result.
- `PERCENT_RANK()` with `DESC` puts the highest earner at 0.0, so checking `<= 0.20` correctly grabs the top 20%.
- Writing the detail table first, then re-reading from Delta to aggregate the summary, guarantees the summary exactly matches what was persisted.
- The `ELSE 'Unknown'` in the CASE WHEN is just good defensive coding.

**WHAT I TWEAKED:**
- The CASE WHEN priority order initially put Inactive before High-Value. That's wrong. If a customer had 0 completed orders but huge revenue from a Pending order (say, a wire transfer), they might still qualify as High-Value. High-Value has to take precedence. I fixed the order.
- The first draft used `NTILE(5)` instead of `PERCENT_RANK()`. NTILE chops the population into 5 exact equal-sized buckets. If the top 20% by count are clustered tightly in revenue, it ignores the natural breakpoints in the data. `PERCENT_RANK()` is much better here. I swapped it.

**WHAT I THREW OUT:**
- It tried to write the summary table directly from the in-memory dataframe before writing the detail table. If Delta did any type coercion on write, the summary and detail could drift. I forced it to write the detail table first, read it back, *then* aggregate.

**VERDICT:**
Accepted 
---

## Prompt 5: Generate create_gold_tables.py (Gold orchestrator)

**PROMPT SENT:**
> [.cursorrules active] Generate src/gold/create_gold_tables.py only as one file , gold orchestrator. same importlib pattern as bronze ingest_all.py. runs 01, 02, 03, 04 in order.
> 
> try/except per script, fail and continue. track start/end time per script and overall.
> 
> master summary table columns: Script, Status, Duration, Gold Tables Created (show just the table names not full paths).
> 
> at the very end print a full inventory of all 6 gold delta tables with their full paths. use a GOLD_TABLE_INVENTORY list for this.
> 
> sys.exit(1) on partial failure. same importlib + sys.path.append pattern as bronze.

**AI RESPONSE:**
It wrote `create_gold_tables.py`. Used a list of dictionaries for the `PIPELINE` so it could track the `tables_created` array for each step. Used `path.split("/")[-1]` to grab just the basename for the summary table. Set up a `GOLD_TABLE_INVENTORY` list to print at the end with little checkmarks or question marks based on success.

**WHAT I KEPT:**
- Using a list of dicts instead of tuples for the PIPELINE was the right call since we needed to track a list of tables per step.
- `path.split("/")[-1]` kept the summary table from blowing out the terminal width.
- The `GOLD_TABLE_INVENTORY` list at the top of the file acts as great documentation for anyone opening the script.
- The `✓` / `?` indicators at the end look super clean in the logs.

**WHAT I TWEAKED:**
- The summary table initially printed the `tables_created` array as a raw python list (like `['table1', 'table2']`). I just had it use `", ".join()` so it looked like a normal string.
- It imported all four modules at the very top of the file. If one had a syntax error, the whole orchestrator would crash before doing anything. I moved the `importlib.import_module()` call into the `try/except` loop so it would fail gracefully and continue.

**WHAT I THREW OUT:**
- It forgot to append the directory to `sys.path` before importing, which breaks the dynamic import for number-prefixed files. It made the same mistake on the Bronze orchestrator. I told it to add it back.
- The final inventory tried to iterate over the execution results to list the tables. But if a script fails, it still has the tables in its dictionary — it just didn't write them. I made it iterate over the static `GOLD_TABLE_INVENTORY` list instead.

**VERDICT:**
Accepted after fixing the imports and the inventory logic. The Gold layer is done , 6 total Delta tables from 4 scripts.
