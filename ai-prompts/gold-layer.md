# AI Prompt Diary — Gold Layer

This file records all AI-assisted interactions related to the Gold layer aggregation scripts.

---

## Prompt 1: Generate 01_sales_by_product.py

**PROMPT SENT:**
> [.cursorrules active] starting gold layer. generate src/gold/01_sales_by_product.py only — one file.
>
> reads from silver_orders and silver_products. gold rule: only use rows where quality_check_result = 'PASS'. never read from bronze or CSVs.
>
> join orders to products on product_id. group by product_id, product_name, category. calculate: total_orders (COUNT order_id), total_revenue (SUM total_amount, rounded 2dp), avg_order_value (AVG total_amount, rounded 2dp). order by total_revenue desc.
>
> write to /FileStore/delta/gold/gold_sales_by_product, mode overwrite.
>
> validation after write: total products with sales, top 5 products by revenue, revenue by category (groupby category → count products, sum revenue).
>
> use spark.sql() with temp views for the aggregation — want it readable as SQL not PySpark dataframe chains. add comments. .py extension not .sql — .sql files can't write delta tables.

**AI RESPONSE SUMMARY:**
Generated `01_sales_by_product.py` with the standard 7-section structure. Filtered PASS rows before `createOrReplaceTempView()` so the temp view only contains clean data. SQL used `ROUND(SUM(...), 2)` and `ROUND(AVG(...), 2)` inline. After writing, re-read from Delta and registered a second temp view `gold_sales_by_product` to run the category breakdown query against the Gold table rather than the Silver one.

**WHAT I ACCEPTED:**
- Filtering PASS rows *before* registering the temp view was the right approach — the view itself only contains clean rows, so the SQL query doesn't need a WHERE clause on `quality_check_result`. Keeps the SQL readable.
- Using a second `createOrReplaceTempView("gold_sales_by_product")` on the re-read Gold table to run the category breakdown query — this ensures the validation is reading from the written Delta table, not from an in-memory DataFrame.
- The category breakdown used `SUM(total_revenue)` on the Gold table (aggregating the already-aggregated per-product revenue), which is correct — it's summing the product-level `total_revenue` column, not re-summing raw order amounts.
- `.py` extension noted in the comment section explaining why `.sql` was not used — useful for anyone reading the file who wonders about the choice.

**WHAT I CHANGED:**
- The initial SQL used `INNER JOIN silver_products p ON o.product_id = p.product_id`. This silently drops orders where `product_id` is NULL — but at this point we've filtered to PASS rows only, and any order with a NULL `product_id` would have failed the completeness check and been excluded already. So INNER JOIN is actually correct here. Left it as INNER JOIN. But added a comment explaining *why* — without it, someone might "fix" it to LEFT JOIN thinking they'd be losing data.
- The validation print initially just called `result_df.show(5)` without `truncate=False`. Long product names would get cut off in the output. Changed to `truncate=False` across all show() calls.

**WHAT I REJECTED:**
- First attempt filtered PASS rows inside the SQL query using `WHERE quality_check_result = 'PASS'` rather than before the temp view registration. Both are logically equivalent, but pushing the filter before the view registration means the temp view is smaller and Spark doesn't need to scan the quality_check_result column during the aggregation join. It's also cleaner — the SQL stays focused on the business logic, not on data quality plumbing.
- It added a `HAVING COUNT(o.order_id) > 0` clause to exclude products with zero orders from the output. Products with zero orders wouldn't appear in an INNER JOIN result anyway — the HAVING clause was redundant and removed.

**FINAL DECISION:**
Accepted after pushing the PASS filter before the temp view and removing the redundant HAVING clause. The `spark.sql()` + temp view pattern was confirmed as the standard for all four Gold aggregation scripts.

---

## Prompt 2: Generate 02_revenue_by_customer.py

**PROMPT SENT:**
> [.cursorrules active] generate src/gold/02_revenue_by_customer.py only. same structure and style as 01_sales_by_product.py.
>
> reads from silver_orders (PASS only) and silver_customers (PASS only). join on customer_id. group by customer_id, customer_name, customer_segment.
>
> metrics: total_orders, total_revenue (rounded 2dp), avg_order_value (rounded 2dp), lifetime_value_actual (carry through from silver_customers — it's a customer attribute, not an aggregated value).
>
> order by total_revenue desc. write to /FileStore/delta/gold/gold_revenue_by_customer.
>
> validation: total customers with orders, top 5 by revenue, revenue breakdown by customer_segment (count, sum revenue, avg revenue per customer).

**AI RESPONSE SUMMARY:**
Generated `02_revenue_by_customer.py` following the same structure. Used `MAX(c.lifetime_value) AS lifetime_value_actual` to carry the customer attribute through the GROUP BY without a separate join step. Comment on the SQL explained the MAX idiom. Segment breakdown in validation used a second `spark.sql()` query against a `gold_revenue_by_customer` temp view.

**WHAT I ACCEPTED:**
- `MAX(c.lifetime_value)` to carry through a non-grouped customer attribute is the standard SQL idiom for this. Since `lifetime_value` is the same for every row of a given `customer_id` after the join (it's a customer-level field), `MAX` just picks the one value that's there. `MIN` or `AVG` would give the same result. The AI commented this clearly: *"We use MAX() to de-aggregate it safely within the GROUP BY — it is the same value for every row of the same customer after the join."* Good explanation, kept it.
- Segment breakdown query in validation used `COUNT(customer_id)`, `SUM(total_revenue)`, and `AVG(total_revenue)` — three meaningful dimensions for understanding segment composition. Useful for the dashboard later.

**WHAT I CHANGED:**
- The join was initially `FROM silver_orders o JOIN silver_customers c` (orders as the left table). This means customers with zero orders are dropped — which is fine for *this* specific aggregation (you can't have revenue metrics for a customer with no orders). But I flipped it to `FROM silver_customers c JOIN silver_orders o` with the customers as the driving table, to make it explicit that the unit of analysis is customers, not orders. Logic is identical with an INNER JOIN either way, but the framing matters for readability.
- The validation `result_df.show(5)` was again without `truncate=False`. Fixed. Customer names can be long.

**WHAT I REJECTED:**
- First version included `c.country` and `c.signup_date` in both the SELECT and GROUP BY. I hadn't asked for these columns and they clutter the Gold table with attributes that aren't needed for the revenue aggregation. The Gold layer should be purposefully shaped for analytics, not a copy of Silver with metrics bolted on. Removed both — `gold_revenue_by_customer` contains only the columns needed for revenue analysis and segmentation.
- It also computed `lifetime_value_actual` using a subquery that re-read `silver_customers` directly as a separate lookup. Unnecessary when `MAX(c.lifetime_value)` inside the existing GROUP BY does the same thing in one query without a second table scan.

**FINAL DECISION:**
Accepted after removing the extra customer attributes and the redundant subquery. The `MAX()` idiom for carrying through non-aggregated attributes was noted — came up again in script 04 for `customer_segment`.

---

## Prompt 3: Generate 03_daily_weekly_trends.py

**PROMPT SENT:**
> [.cursorrules active] generate src/gold/03_daily_weekly_trends.py only.
>
> reads from silver_orders only. two filters: quality_check_result = 'PASS' AND order_status = 'Completed'. only completed orders represent confirmed revenue for trend analysis.
>
> daily trends: group by order_date → daily_orders (COUNT), daily_revenue (SUM rounded 2dp), daily_avg_order_value (AVG rounded 2dp). order by order_date asc.
>
> weekly trends: group by year + week number → use YEAR() and WEEKOFYEAR() spark sql functions. columns: order_year, week_number, weekly_orders, weekly_revenue, weekly_avg_order_value. order by year asc then week asc.
>
> write both as separate gold delta tables: gold_daily_trends and gold_weekly_trends. validation: date range of completed orders (min/max date, total days), total day-level rows, total week-level rows, top 5 highest revenue days.

**AI RESPONSE SUMMARY:**
Generated `03_daily_weekly_trends.py`. Applied both filters chained: `.filter("quality_check_result = 'PASS'").filter("order_status = 'Completed'")` before registering the temp view `silver_orders_completed`. Daily aggregation used `ORDER BY order_date ASC`. Weekly used `GROUP BY YEAR(order_date), WEEKOFYEAR(order_date)` with aliases `order_year` and `week_number`, ordered by `order_year ASC, week_number ASC`. Both tables written to separate Gold paths. Validation queried date range from `gold_daily_trends` temp view.

**WHAT I ACCEPTED:**
- Naming the temp view `silver_orders_completed` (not just `silver_orders`) was a good clarification — it signals clearly that this view is already doubly filtered, so the SQL query doesn't need additional WHERE clauses. Avoids confusion if someone reads the SQL and wonders about the filter state.
- Writing daily and weekly as two separate Gold tables rather than a single combined one was the right decision. Dashboard tools work better with pre-shaped single-purpose tables than with mixed-granularity data. Also makes it easier to read daily vs. weekly from the dashboard query script.
- The weekly GROUP BY uses `YEAR(order_date)` as well as `WEEKOFYEAR()` — this is important. `WEEKOFYEAR(order_date)` alone would merge week 1 of different years into a single row. The `YEAR()` grouping key prevents that.

**WHAT I CHANGED:**
- First version ordered weekly results by `week_number ASC` only, without `order_year`. For a multi-year dataset, this would sort all "week 1" rows together regardless of year, then all "week 2" rows, etc. — completely wrong for a time series. Added `ORDER BY order_year ASC, week_number ASC` to get chronological ordering.
- The top-5 revenue days validation initially used `result_df.orderBy(col("daily_revenue").desc()).show(5)` on the in-memory DataFrame. Changed to a `spark.sql()` query on the registered `gold_daily_trends` temp view — consistent with how the rest of the Gold scripts do validation, and confirms the written Delta data is being read rather than the computed DataFrame.

**WHAT I REJECTED:**
- The first attempt used a single combined filter `.filter("quality_check_result = 'PASS' AND order_status = 'Completed'")`. Functionally identical to two chained filters, but two explicit `.filter()` calls is clearer — each filter corresponds to a distinct logical rule (data quality gate, then business filter). Kept as two separate chains.
- It suggested adding a Pending/Cancelled breakdown in the validation section to show "how many orders were excluded". I removed it — the validation section should confirm what *was* written, not explain what was filtered out. That diagnostic belongs in the Silver layer, not Gold.

**FINAL DECISION:**
Accepted after fixing the weekly ordering and switching the top-5 validation to a SQL query on the Gold temp view. The `silver_orders_completed` view naming convention for doubly-filtered data was a useful pattern noted for script 04.

---

## Prompt 4: Generate 04_customer_segmentation.py

**PROMPT SENT:**
> [.cursorrules active] generate src/gold/04_customer_segmentation.py only — this is the most complex gold script, read carefully.
>
> reads from silver_orders (PASS only) and silver_customers (PASS only).
>
> step 1: compute per-customer metrics. LEFT JOIN customers to orders on customer_id. calculate total_completed_orders (COUNT of Completed orders only) and total_revenue (SUM total_amount for Completed orders only). customers with zero completed orders must be included — they become Inactive. use COALESCE for their revenue so they get 0 not NULL.
>
> step 2: PERCENT_RANK() ordered by total_revenue DESC. rank 0.0 = highest earner.
>
> step 3: CASE WHEN in this exact priority order: High-Value (percent_rank <= 0.20), Inactive (total_completed_orders = 0), Repeat (total_completed_orders >= 2), One-Time (total_completed_orders = 1). add ELSE 'Unknown' as a safety net.
>
> step 4: aggregate by segment_type → customer_count, avg_revenue, total_revenue.
>
> write customer-level detail to gold_customer_segment_detail FIRST. re-read from delta, then aggregate summary and write to gold_customer_segmentation. validation: total customers segmented, count per segment_type, confirm 0 rows with NULL or Unknown segment.

**AI RESPONSE SUMMARY:**
Generated `04_customer_segmentation.py` with four clearly separated steps (compute metrics → PERCENT_RANK → CASE WHEN assign → aggregate). LEFT JOIN from `silver_customers` to `silver_orders` with `COUNT(CASE WHEN o.order_status = 'Completed' THEN 1 END)` and `COALESCE(SUM(CASE WHEN ... THEN o.total_amount END), 0)`. PERCENT_RANK window: `PERCENT_RANK() OVER (ORDER BY total_revenue DESC)`. CASE WHEN applied the four segments in the specified priority order with `ELSE 'Unknown'`. Detail table written first, re-read from Delta, registered as temp view, then summary aggregated from that view. Validation checked for NULL/Unknown segments.

**WHAT I ACCEPTED:**
- LEFT JOIN from `silver_customers` to `silver_orders` (not the other way around) was essential. Inactive customers have zero orders — an INNER JOIN or orders-first join would drop them entirely, making it impossible to classify them. The join direction was correct and commented clearly.
- `COUNT(CASE WHEN o.order_status = 'Completed' THEN 1 END)` and `COALESCE(SUM(CASE WHEN ... THEN o.total_amount END), 0)` in a single LEFT JOIN query was more efficient than two separate queries or sub-selects. The CASE WHEN inside the aggregate functions correctly counts/sums only Completed orders while keeping all customers in the result.
- `PERCENT_RANK() OVER (ORDER BY total_revenue DESC)` with DESC means rank 0.0 = top earner, rank 1.0 = lowest. So `percent_rank <= 0.20` captures the top 20%. This is the correct interpretation. An ASC ordering would have inverted this — rank 0.0 would be the lowest earner and the top-20% condition would be broken.
- Writing the detail table first, then re-reading from Delta to aggregate the summary, was the right sequencing. It guarantees the summary is computed from the exact same data that was persisted, not from an in-memory DataFrame that might differ if there were any write-side transformations.
- The `ELSE 'Unknown'` safety net caught my attention — the four CASE WHEN branches are mathematically exhaustive (every customer has either 0, 1, or ≥2 completed orders, and High-Value overlaps with any of those) but an `ELSE 'Unknown'` makes the logic robust against edge cases like a customer with NULL `total_completed_orders` after the aggregation. Good defensive coding.

**WHAT I CHANGED:**
- The CASE WHEN priority order in the first attempt was: Inactive → High-Value → Repeat → One-Time. This is wrong. A customer with 0 completed orders but a very high `total_revenue` from a single non-completed order might have a `percent_rank` that qualifies for High-Value. With Inactive first, they'd get classified as Inactive — which is technically correct for this dataset (since revenue is calculated from Completed orders only, someone with 0 Completed orders will always have `total_revenue = 0`, putting them at rank 1.0, well outside the top 20%). But the specified priority is High-Value first — if a customer qualifies as High-Value, that takes precedence regardless. Changed the CASE WHEN order to: High-Value → Inactive → Repeat → One-Time, exactly as specified.
- The initial segmentation query used `NTILE(5)` to rank customers instead of `PERCENT_RANK()`. NTILE(5) divides the customer population into five equal-sized buckets — bucket 1 = top 20%. The problem is that NTILE forces exactly equal group sizes regardless of revenue distribution. If the top 20% of customers by count are actually clustered at similar revenue levels, NTILE still cuts at exactly 20% of the row count, which doesn't reflect meaningful revenue gaps. PERCENT_RANK() is based on actual revenue values, so a natural breakpoint in the revenue distribution is respected. Replaced NTILE with PERCENT_RANK, explained the reasoning in the comment.

**WHAT I REJECTED:**
- First version wrote the summary table directly from the in-memory `segmentation_summary_df` DataFrame without first writing and re-reading the detail table. This means the summary was computed from `customer_segmented_df` (an in-memory result) rather than from the Delta-persisted `gold_customer_segment_detail`. If any data type coercion or rounding happened at write time (Delta often does this with DecimalType columns), the in-memory and persisted data could differ slightly. The correct sequence is: write detail → read from Delta → aggregate summary → write summary. Fixed the ordering.
- The validation initially didn't cross-check the segment counts between the summary table and the detail table. I asked it to add `detail_result.groupBy("segment_type").count()` alongside the summary table output — so there are two independent counts of customers per segment, one from each Gold table. If they don't match, something went wrong in the aggregation step.

**FINAL DECISION:**
Accepted after fixing the CASE WHEN priority order (High-Value first), replacing NTILE with PERCENT_RANK, and enforcing detail-first write sequencing. The NTILE vs PERCENT_RANK decision was the most technically significant choice in the whole Gold layer.

---

## Prompt 5: Generate create_gold_tables.py (Gold orchestrator)

**PROMPT SENT:**
> [.cursorrules active] generate src/gold/create_gold_tables.py — gold orchestrator. same importlib pattern as bronze ingest_all.py. runs 01, 02, 03, 04 in order.
>
> try/except per script, fail and continue. track start/end time per script and overall.
>
> master summary table columns: Script, Status, Duration, Gold Tables Created (show just the table names not the full paths).
>
> at the very end print a full inventory of all 6 gold delta tables with their full paths. use a GOLD_TABLE_INVENTORY list for this.
>
> sys.exit(1) on partial failure. same importlib + sys.path.append pattern as bronze.

**AI RESPONSE SUMMARY:**
Generated `create_gold_tables.py`. The `PIPELINE` list used dicts instead of tuples (to carry the extra `tables_created` list per step). Each dict had `display_name`, `module_name`, and `tables_created`. The summary table printed just the table basename (`path.split("/")[-1]`) rather than full paths. A separate `GOLD_TABLE_INVENTORY` constant listed all 6 paths for the end-of-run inventory print. The inventory section used `✓` or `?` as the status indicator depending on `all_succeeded`.

**WHAT I ACCEPTED:**
- Using a list of dicts for `PIPELINE` instead of tuples was the right call for this orchestrator — each step carries three pieces of metadata (name, module, tables list) and a dict makes that more readable than a tuple with a list nested inside. The Bronze orchestrator used tuples of two items, which was fine for that case. For Gold with the extra `tables_created` per step, dicts are cleaner.
- `path.split("/")[-1]` to extract the table basename for the summary table column was simple and correct. Full paths in a summary row would overflow the column width and be redundant — the inventory section at the end shows full paths.
- The `GOLD_TABLE_INVENTORY` constant at the top of the file doubles as documentation — anyone reading the file immediately sees all 6 Gold table paths without running the script.
- `✓` / `?` status indicators in the inventory section were a nice touch — `✓` if everything succeeded, `?` if there were failures (some tables might not have been written).

**WHAT I CHANGED:**
- The initial summary table printed `tables_created` as a Python list representation: `['/FileStore/delta/gold/gold_daily_trends', '/FileStore/delta/gold/gold_weekly_trends']`. That's unreadable in a fixed-width terminal table. Changed to `", ".join([t.split("/")[-1] for t in r["tables_created"]])` to show just the table names, comma-separated.
- The orchestrator initially imported all four modules at the top of the file using `importlib.import_module()` before `run_pipeline()` was called. This means if any module fails to import (e.g., a syntax error in one file), the entire orchestrator crashes before running anything. Moved the `importlib.import_module()` call inside the `try/except` block within the loop, so module import failure is caught per-script and the pipeline continues. Same fix as Bronze's orchestrator in terms of fail-and-continue intent.

**WHAT I REJECTED:**
- First attempt didn't include `sys.path.append(os.path.dirname(__file__))` at the top. The Gold scripts have number-prefixed names, same as Bronze — without the path append, `importlib.import_module("01_sales_by_product")` will fail with a `ModuleNotFoundError` unless the script's directory is on `sys.path`. This was the exact same issue from the Bronze orchestrator, and I'd specifically called it out in the Bronze diary. Added it back.
- The final inventory section initially iterated over `results` (the per-script execution results) to list tables. The problem is that FAILED scripts still have `tables_created` listed in their dict — they just didn't actually write them. Iterating `results` and marking failed scripts' tables as created would be misleading. Changed to iterating the static `GOLD_TABLE_INVENTORY` list, with the `✓` / `?` indicator reflecting overall pipeline success rather than per-table confirmation.

**FINAL DECISION:**
Accepted after fixing `sys.path.append`, moving `importlib.import_module` inside the try/except, and switching the inventory to use `GOLD_TABLE_INVENTORY` instead of `results`. The Gold layer was complete — 6 total Delta tables across 4 aggregation scripts and 1 orchestrator, all reachable via `create_gold_tables.py`.
