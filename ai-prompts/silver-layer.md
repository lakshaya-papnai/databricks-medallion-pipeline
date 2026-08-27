# AI Prompt Diary — Silver Layer

This file records the chat history and decisions made while designing the Silver layer quality check scripts.

---

## Prompt 1: Generate 01_quality_completeness.py

**PROMPT SENT:**
> [.cursorrules active] Starting silver layer. generate src/silver/01_quality_completeness.py only as one file.
> 
> reads from bronze_customers and bronze_orders delta tables (NOT raw CSVs). writes to silver_customers_completeness and silver_orders_completeness at /FileStore/delta/silver/
> 
> hard rule: never delete bad rows. add quality_check_result col — PASS or FAIL with a reason string.
> 
> customers check: flag NULL email -> "FAIL - NULL email". everything else passes.
> 
> orders checks: flag NULL customer_id -> "FAIL - NULL customer_id". flag NULL product_id -> "FAIL - NULL product_id". a single row can fail BOTH — use concat_ws() to combine reasons on the same row, don't use separate cols.
> 
> after writing: read back and print pass count, fail count, pass %, and individual expected-vs-actual counts. expected: 50 NULL emails, 100 NULL customer_ids, 200 NULL product_ids.
> 
> same comments as bronze scripts. mode overwrite.

**AI RESPONSE:**
It wrote `01_quality_completeness.py` with the check functions and added a nice `print_quality_report()` helper to print a 5-column table (`check_name | total | passed | failed | pass_%`). For the orders check, it nailed the `concat_ws` logic by wrapping each check in a `when().otherwise(lit(None))`, which perfectly handles rows that fail both checks without leaving weird trailing commas for rows that only fail one.

**WHAT I KEPT:**
- The `print_quality_report()` helper was a great idea. Way cleaner than copying the print statements into every function. I made it standard across the Silver layer.
- The `concat_ws` approach was spot on. 
- Reading back from Delta to validate instead of just checking the dataframe memory state. Good pattern from the bronze layer carried over.

**WHAT I TWEAKED:**
- It passed validation counts into the report helper. I told it to pull those out and keep the helper generic. The specific expected-vs-actual counts should just live in the main check functions.

**WHAT I THREW OUT:**
- It tried to enforce non-nulls by dropping rows right at the top (`df.filter(customer_id.isNotNull())`). Absolutely not. That violates the #1 rule of the Silver layer (never delete data). I ripped that out immediately.
- It also threw in a `"FAILFAST"` option on the Bronze read, which would crash the script if the schema was messy. Bronze *is* messy by design, so that's too aggressive. Deleted the option.

**VERDICT:**
Accepted after ripping out the `FAILFAST` and row-dropping logic. The report helper and `concat_ws` pattern became the template.

---

## Prompt 2: Generate 02_quality_uniqueness.py

**PROMPT SENT:**
> [.cursorrules active] generate src/silver/02_quality_uniqueness.py only.
> 
> reads from bronze_customers and bronze_orders. writes to silver_customers_uniqueness and silver_orders_uniqueness at /FileStore/delta/silver/
> 
> use ROW_NUMBER() window function. partition by customer_id (or order_id). order by ingestion_timestamp. first occurrence = PASS, any subsequent row with same PK = "FAIL - DUPLICATE customer_id" (or order_id).
> 
> drop the ROW_NUMBER helper col before writing — shouldn't be in the output table.
> 
> same print_quality_report helper style, same expected/actual print. expected: 10 duplicate customer_ids, 20 duplicate order_ids.

**AI RESPONSE:**
It wrote the script using `Window.partitionBy("customer_id").orderBy("ingestion_timestamp")`. Added a `_row_num` helper column, flagged anything `> 1` as a duplicate, and dropped the column right before the write. 

**WHAT I KEPT:**
- Using `ROW_NUMBER()` ordered by `ingestion_timestamp` was the right move. The first ingested row wins and the rest get flagged. It's deterministic.
- Dropping `_row_num` *before* the Delta write was correct so the table stays clean.
- The leading underscore on `_row_num` is a nice convention to signal it's internal.

**WHAT I TWEAKED:**
- Nothing

**WHAT I THREW OUT:**
- On the very first pass, it actually tried to use `dropDuplicates(["customer_id"])` because it was "simpler". Totally misses the point. `dropDuplicates` removes rows, which breaks our rules. I told it to switch to `ROW_NUMBER` so we can just flag them.
- It added a comment saying "in production you might want to keep only PASS rows downstream". That's what the Gold layer is for, not Silver. Deleted the comment since it contradicted our architecture.

**VERDICT:**
Accepted after fixing the `dropDuplicates` mistake. This `ROW_NUMBER` pattern came in handy later during the Silver table joins.

---

## Prompt 3: Generate 03_quality_type_validation.py

**PROMPT SENT:**
> [.cursorrules active] Generate src/silver/03_quality_type_validation.py only.
> 
> reads from all three bronze tables. writes to silver_customers_type_validation, silver_orders_type_validation, silver_products_type_validation.
> 
> customers: signup_date must not be null (it's already DateType, so just check isNull), lifetime_value >= 0
> orders: order_date not null, quantity >= 1, unit_price >= 0, total_amount >= 0
> products: price >= 0, cost >= 0, stock_quantity >= 0, reorder_level >= 0, and cost must be strictly < price
> 
> concat_ws for multi-failure rows. expected: 0 failures on all three tables (data was gen clean for type rules). print that.
> 
> same comments, same print_quality_report helper.

**AI RESPONSE:**
It wrote the script with three check functions. It built individual boolean flags for each condition and then fed them all into the `concat_ws` block. For products, it correctly framed the domain rule as `cost >= price` to trigger the failure. Validation printed 0 expected failures.

**WHAT I KEPT:**
- Breaking out the boolean conditions into separate variables (like `amount_null = col("order_date").isNull()`) before the `when()` chain was super readable. Way better than one massive inline blob.
- The framing for `cost >= price` as the failure condition was smart. 
- Printing `(expected: 0)` was helpful to confirm the data generator actually worked as intended.

**WHAT I TWEAKED:**
- It initially checked `order_status` in the orders function (making sure it was Pending/Completed/Cancelled). I actually moved that over to the business logic script instead. Type validation should just be numbers and dates, not enum domains.
- I changed its `quantity > 0` check to `quantity >= 1`. Same thing for integers, but `>= 1` reads better for an order line.

**WHAT I THREW OUT:**
- It tried to re-cast the `signup_date` using `.cast(DateType())` to catch parse errors. Redundant because the Bronze schema already enforces it. If it made it this far, it's a date. I just pulled the cast out.

**VERDICT:**
Accepted after removing the redundant cast and moving the enum check to business logic.

---

## Prompt 4: Generate 04_quality_referential_integrity.py

**PROMPT SENT:**
> [.cursorrules active] generate src/silver/04_quality_referential_integrity.py only.
> 
> reads from bronze_orders, bronze_customers, bronze_products. writes to silver_orders_referential_integrity at /FileStore/delta/silver/
> 
> logic: LEFT JOIN bronze_orders to bronze_customers on customer_id. if customer side is NULL but order's customer_id is NOT NULL -> orphan customer_id.
> LEFT JOIN same result to bronze_products on product_id. same orphan logic.
> 
> important: rows where customer_id or product_id is already NULL must NOT be flagged here, those are completeness failures. only flag rows where the FK exists but points to nothing.
> 
> use concat_ws for rows orphaned on both FKs. expected: 50 orphan customer_ids, 30 orphan product_ids.
> 
> drop the join helper cols before writing. same report helper.

**AI RESPONSE:**
It wrote the script using two chained `LEFT JOIN`s. Crucially, it threw a `.distinct()` on the parent tables before the join. It used `col("customer_id").isNotNull() & col("cust_valid_id").isNull()` to properly flag orphans without catching the NULLs. Dropped the helper columns right before writing.

**WHAT I KEPT:**
- The `.distinct()` on the parent tables was a massive save. `bronze_customers` has duplicate IDs planted in it. If we joined without `.distinct()`, it would have caused a Cartesian fan-out and corrupted the row counts for the whole layer. It caught that without me prompting it.
- Aliasing the parent IDs (`cust_valid_id`, etc.) prevented column ambiguity during the join.
- The `isNotNull()` guard was exactly what I wanted. Keeps completeness and ref-integrity failures clearly separated.

**WHAT I TWEAKED:**
- In the validation print section, it tried to count failures using an exact match (`col == "FAIL - ORPHAN..."`). That breaks if a row fails both joins and has a concatenated string. Swapped it to `.contains(...)` so it catches dual-failure rows.

**WHAT I THREW OUT:**
- It originally tried to use an `INNER JOIN` to find matches, and then math it out (`total - matched = orphans`). Logically works, but falls apart when FKs can be NULL. I made it rewrite it using the `LEFT JOIN` logic.

**VERDICT:**
Accepted after switching to `LEFT JOIN` and fixing the `.contains()` filter. The `.distinct()` trick was something I kept in mind for the final Gold joins.

---

## Prompt 5: Generate 05_quality_business_logic.py

**PROMPT SENT:**
> [.cursorrules active] generate src/silver/05_quality_business_logic.py only.
> 
> orders business logic checks:
> - total_amount must equal quantity * unit_price, allow tolerance of 0.01 for rounding (use abs())
> - payment_date must be NULL if order_status is Pending or Cancelled
> - payment_date must NOT be NULL if order_status is Completed
> - order_date must not be in the future (use current_date())
> 
> customers business logic checks:
> - customer_segment must be Premium / Standard / Basic
> - signup_date must not be in the future
> - lifetime_value must be > 0
> 
> concat_ws for multi-failure rows. expected 0 failures for both tables. same helper, same comments.

**AI RESPONSE:**
Wrote the script perfectly. For the `total_amount` check, it did `spark_round(quantity * unit_price, 2)` before checking the absolute difference. The `payment_date` logic and customer segments checks used `.isin()`, and it used `current_date()` for the future-date checks. 

**WHAT I KEPT:**
- Adding `spark_round()` before doing the `abs()` comparison was super smart. Float math can get weird (`29.9999` vs `30.00`), and this prevented false positive flags.
- Naming all the boolean conditions before building the `concat_ws` made it very readable again.
- Using `current_date()` evaluated at Spark runtime instead of hardcoding a Python `date.today()` literal was correct for a pipeline that would normally be scheduled.

**WHAT I TWEAKED:**
- The `lifetime_value > 0` check initially used `col("lifetime_value") <= 0`, which evaluates to NULL when the field is NULL (meaning it would technically PASS). Our data doesn't have NULLs here, but I tweaked it to `isNotNull() & (<= 0)` just to be safe.

**WHAT I THREW OUT:**
- It tried to add a check making sure `order_status` was valid. I actually *had* moved it here from script 03, but realized since the generator guarantees valid statuses, checking it here doesn't actually do anything. It's more of a documentation rule at this point. I just ripped it out to keep the script lean.
- It suggested wrapping the future-date check in a NULL guard. Technically true, but NULL dates get flagged upstream in script 03 anyway, and having multiple failure flags on a row is fine. Deleted it.

**VERDICT:**
Accepted after adjusting the `lifetime_value` check and removing the extra status check. 

---

## Prompt 6: Generate create_silver_tables.py (Silver orchestrator)

**PROMPT SENT:**
> [.cursorrules active] Now gen src/silver/create_silver_tables.py , the silver orchestrator. this is the complex one so read carefully.
> 
> reads all the individual quality-check silver tables per entity, joins them together, and produces three final consolidated silver tables: silver_customers, silver_orders, silver_products
> 
> joining logic: join on PK + ingestion_timestamp (both cols, not just PK). reason: if bronze has duplicate customer_ids, joining only on customer_id would fan out and wreck the row count. ingestion_timestamp acts as the tiebreaker.
> 
> after joining: combine all quality_check_result cols into one final quality_check_result. if ALL = PASS -> final = PASS. if ANY fail -> concat all failure reasons with " | " separator.
> 
> customers to join: completeness, uniqueness, type, business (4 tables)
> orders to join: completeness, uniqueness, type, ref_integrity, business (5 tables)
> products: only type was run — just promote it directly, no join needed
> 
> write final tables to /FileStore/delta/silver/ with mode overwrite
> print master report at end: table | total_rows | passed_rows | failed_rows | pass_percentage
> expected: ~60 failed in customers, ~400 failed in orders, 0 failed in products
> 
> use same try/except orchestrator pattern as bronze ingest_all.py
> sys.exit(1) on partial failure

**AI RESPONSE:**
It wrote a generic `combine_quality_flags` helper function that takes a list of `(alias, path)` tuples and iterates through them, doing a `LEFT JOIN` on the provided join keys. It grabs all the `qc_*` columns and squashes them into one final result using `concat_ws`. If it's empty, it outputs `"PASS"`. Products just got a direct read/write since it was only one table.

**WHAT I KEPT:**
- The `combine_quality_flags` helper was brilliant. Hardcoding 5 different joins for orders would have been messy. Passing a list of tuples makes it scalable.
- Joining on `[customer_id, ingestion_timestamp]` worked perfectly to prevent row fan-out from the Bronze duplicates.
- Products just doing a direct promote was clean and simple.

**WHAT I TWEAKED:**
- In the join helper, it originally just passed a list of strings to `join()`. This breaks when both tables have the exact same columns (like `source_file_name`) because Spark gets confused. I had it add `.select(*right_cols)` so it only brings over the join keys and the quality flag.
- The failure detection originally called `concat_ws` twice inline. I had it pull the expression into a variable to clean up the code.

**WHAT I THREW OUT:**
- It forgot to add the `try/except` orchestrator pattern I asked for at first. I made it go back and wrap the entity functions just like the Bronze layer so jobs don't silently fail.

**VERDICT:**
Accepted after fixing the join type and adding the error handling. That `combine_quality_flags` function is probably the best piece of reusable code in the whole Silver layer.
