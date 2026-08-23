# AI Prompt Diary — Silver Layer

This file records all AI-assisted interactions related to the Silver layer quality check scripts.

---

## Prompt 1: Generate 01_quality_completeness.py

**PROMPT SENT:**
> [.cursorrules active] starting silver layer. generate src/silver/01_quality_completeness.py only — one file.
>
> reads from bronze_customers and bronze_orders delta tables (NOT raw CSVs — .cursorrules rule). writes to silver_customers_completeness and silver_orders_completeness at /FileStore/delta/silver/
>
> hard rule: never delete bad rows. add quality_check_result column — PASS or FAIL with a reason string.
>
> customers check: flag NULL email → "FAIL - NULL email". everything else passes.
>
> orders checks: flag NULL customer_id → "FAIL - NULL customer_id". flag NULL product_id → "FAIL - NULL product_id". a single row can fail BOTH — use concat_ws() to combine reasons on the same row, not separate columns.
>
> after writing each table: read back and print pass count, fail count, pass %, individual expected-vs-actual counts. expected: 50 NULL emails, 100 NULL customer_ids, 200 NULL product_ids.
>
> same commenting style as bronze scripts. mode overwrite.

**AI RESPONSE SUMMARY:**
Generated `01_quality_completeness.py` with two check functions (`check_customers_completeness`, `check_orders_completeness`) and a shared `print_quality_report()` helper. The helper printed the five-column table: `check_name | total | passed | failed | pass_%`. For orders, `concat_ws(", ", when(...).otherwise(lit(None)), when(...).otherwise(lit(None)))` correctly built the composite failure string — rows failing both checks got `"FAIL - NULL customer_id, FAIL - NULL product_id"`, not two separate columns. The outer `when()` triggered on `customer_id.isNull() | product_id.isNull()` before delegating to `concat_ws`.

**WHAT I ACCEPTED:**
- The `print_quality_report()` helper abstraction was cleaner than inlining the same five-column print block in every function. Decided to use this pattern across all five Silver check scripts.
- The `concat_ws()` approach for multi-failure rows was exactly what I'd asked for and what `.cursorrules` specifies. Importantly, wrapping each failure message in `when(...).otherwise(lit(None))` means `concat_ws` silently skips nulls — so a row failing only one check doesn't end up with a trailing comma or empty string in the reason.
- Reading back from Delta after the write for validation (same pattern as Bronze) rather than inspecting the in-memory DataFrame — important for confirming the write actually landed.

**WHAT I CHANGED:**
- The initial version had the outer `when()` condition duplicating the inner conditions verbatim: `when(col("customer_id").isNull() | col("product_id").isNull(), concat_ws(...))`. This was fine logically, but I noticed the `concat_ws` with the `otherwise(lit(None))` guards already handles the "neither fails" case by returning an empty string — the outer `when` felt redundant. Asked it to keep the outer `when` for explicitness but confirmed this was a style choice, not a bug. Left it in because it makes the intent clearer.
- The `print_quality_report()` function initially took a `df` and a `check_name` string. The validation counts (e.g., `null_email_count`) were printed inline after calling the helper. I asked it to keep these outside the helper — the helper should only print the aggregate pass/fail table; specific count validation stays in the check functions so the helper stays reusable.

**WHAT I REJECTED:**
- The first attempt had `nullable=False` constraints enforced via a DataFrame filter at the top of each check function — dropping rows where `customer_id IS NULL` before running any check logic. This directly violates the `.cursorrules` hard rule (never delete bad rows). The whole point is that NULL rows stay in the Silver table, just flagged. Removed entirely.
- It added an `.option("mode", "FAILFAST")` on the Bronze Delta read — which would abort the script if any schema mismatch was encountered. That's too aggressive for a Bronze table that intentionally has dirty data. Changed to the default read behaviour with no mode option.

**FINAL DECISION:**
Accepted after removing the filter-on-read and FAILFAST option. The `print_quality_report()` helper and `concat_ws` pattern were locked in as the standard for all subsequent Silver check scripts.

---

## Prompt 2: Generate 02_quality_uniqueness.py

**PROMPT SENT:**
> [.cursorrules active] generate src/silver/02_quality_uniqueness.py only.
>
> reads from bronze_customers and bronze_orders. writes to silver_customers_uniqueness and silver_orders_uniqueness at /FileStore/delta/silver/
>
> use ROW_NUMBER() window function. partition by customer_id (or order_id). order by ingestion_timestamp. first occurrence = PASS, any subsequent row with the same PK = "FAIL - DUPLICATE customer_id" (or order_id).
>
> drop the ROW_NUMBER helper column before writing — it's implementation detail, shouldn't be in the output table.
>
> same print_quality_report helper style, same expected/actual print. expected: 10 duplicate customer_ids, 20 duplicate order_ids.

**AI RESPONSE SUMMARY:**
Generated `02_quality_uniqueness.py` with `check_customers_uniqueness` and `check_orders_uniqueness` functions. Window spec: `Window.partitionBy("customer_id").orderBy("ingestion_timestamp")`. Used `row_number().over(window_spec)` into a `_row_num` helper column, then `when(col("_row_num") > 1, lit("FAIL - DUPLICATE customer_id")).otherwise(lit("PASS"))` for the quality flag. Dropped `_row_num` with `.drop("_row_num")` before the Delta write. Same `print_quality_report()` helper as script 01.

**WHAT I ACCEPTED:**
- `ROW_NUMBER()` ordered by `ingestion_timestamp` was the right choice for "which duplicate survives as PASS". The first row ingested is treated as canonical; any later row with the same PK is the duplicate. This is consistent with how the Bronze layer timestamp works and deterministic across re-runs.
- Dropping `_row_num` before writing was correctly done before the write, not after — so the helper column never appears in the Silver Delta table at all.
- The `_row_num` naming convention with the leading underscore signals "internal helper" clearly to anyone reading the code.

**WHAT I CHANGED:**
- First version ordered the window by `col("ingestion_timestamp").asc()` explicitly. The `.asc()` is the default so it's technically redundant, but I kept it in the comment explaining the window spec because it makes the intent explicit — "first ingested = PASS". Felt worth the verbosity here.
- The initial `check_orders_uniqueness` function partitioned by `col("order_id")` using the column object syntax while `check_customers_uniqueness` used the string `"customer_id"`. Inconsistent. Standardised both to string syntax to match the rest of the Silver scripts.

**WHAT I REJECTED:**
- The first attempt used `dropDuplicates(["customer_id"])` as an alternative approach to uniqueness checking, noting it was "simpler". This is completely wrong for this pipeline — `dropDuplicates` *removes* rows, which violates the hard rule. The whole point is that duplicate rows stay in the table, flagged with `FAIL - DUPLICATE customer_id`. ROW_NUMBER() is the only correct approach here. Flagged this immediately and it was corrected on the next response.
- It also initially added a comment suggesting that "in production you might want to keep only PASS rows downstream". That's downstream Gold's concern, not Silver's. Silver's job is to flag, not filter. Removed the comment — it contradicts the architecture.

**FINAL DECISION:**
Accepted after removing `dropDuplicates` and the misleading comment. The `ROW_NUMBER()` + `_row_num` drop pattern was correct and reused in `create_silver_tables.py` when joining on PK + `ingestion_timestamp`.

---

## Prompt 3: Generate 03_quality_type_validation.py

**PROMPT SENT:**
> [.cursorrules active] generate src/silver/03_quality_type_validation.py only.
>
> reads from all three bronze tables. writes to silver_customers_type_validation, silver_orders_type_validation, silver_products_type_validation.
>
> customers: signup_date must not be null after cast (it's already DateType from bronze schema, so check isNull), lifetime_value >= 0
> orders: order_date not null, quantity >= 1, unit_price >= 0, total_amount >= 0
> products: price >= 0, cost >= 0, stock_quantity >= 0, reorder_level >= 0, and cost must be strictly < price (domain rule)
>
> concat_ws for multi-failure rows same as before. expected: 0 failures on all three tables (data was generated clean for type rules). print that.
>
> same commenting style, same print_quality_report helper.

**AI RESPONSE SUMMARY:**
Generated `03_quality_type_validation.py` with three separate check functions. For each table, it built individual boolean failure flags, then combined them using `concat_ws` with `when(...).otherwise(lit(None))` wrappers. The products `cost < price` check used `col("cost") >= col("price")` as the failure condition. Expected failure count for all three tables was 0 — confirmed in the validation print. Used aliased imports `min as spark_min` and `max as spark_max` where needed (carried from the Bronze products profiling pattern).

**WHAT I ACCEPTED:**
- Separate boolean variables for each failure condition before the `concat_ws` block was much more readable than embedding all the conditions inline. e.g., `amount_null = col("order_date").isNull()` then using `amount_null` in the `when()` chain. Easy to add or remove checks later.
- The products `cost >= price` as the failure condition (meaning: cost is NOT strictly less than price) was the right framing — it's a domain integrity rule that cost must always be below price, not a type check per se, but placing it here made sense since it's a value-level constraint on numeric fields.
- Printing `(expected: 0)` for all failure counts was the right call — confirms the check is working and the data generator respected the type constraints.

**WHAT I CHANGED:**
- The orders type check initially included `order_status` validation — checking it was one of "Pending", "Completed", "Cancelled". I moved this out of type validation and into business logic (script 05), where domain value checks belong. Type validation should cover numeric ranges and non-null date fields, not enum membership.
- The `quantity >= 1` check for orders was initially written as `col("quantity") > 0` — semantically identical for integers, but `>= 1` is clearer in intent (quantity of 0 is meaningless for an order line). Changed to `>= 1` to match the spec.

**WHAT I REJECTED:**
- First attempt tried to re-cast the `signup_date` column in customers using `col("signup_date").cast(DateType())` to check for parse errors. The Bronze schema already enforces `DateType` explicitly — if a row made it to Bronze with a valid `signup_date` value, it's already a proper date. Re-casting it in Silver is redundant and confused type validation (schema-level) with completeness (null-level). The NULL check is sufficient. Removed the cast.

**FINAL DECISION:**
Accepted after removing the re-cast and moving `order_status` enum check to business logic. Zero expected failures confirmed the data generation was clean for type constraints.

---

## Prompt 4: Generate 04_quality_referential_integrity.py

**PROMPT SENT:**
> [.cursorrules active] generate src/silver/04_quality_referential_integrity.py only.
>
> reads from bronze_orders, bronze_customers, bronze_products. writes to silver_orders_referential_integrity at /FileStore/delta/silver/
>
> logic: LEFT JOIN bronze_orders to bronze_customers on customer_id. if the customer side of the join is NULL but the order's customer_id is NOT NULL → orphan customer_id.
> LEFT JOIN same result to bronze_products on product_id. same orphan logic.
>
> important: rows where customer_id or product_id is already NULL must NOT be flagged here — those are completeness failures, not referential integrity failures. only flag rows where the FK exists but points to a non-existent parent.
>
> use concat_ws for rows that are orphaned on both FKs. expected: 50 orphan customer_ids, 30 orphan product_ids.
>
> drop the join helper columns before writing. same quality report helper.

**AI RESPONSE SUMMARY:**
Generated `04_quality_referential_integrity.py`. Used `.select(col("customer_id").alias("cust_valid_id")).distinct()` on `bronze_customers` before joining — to avoid Cartesian product from Bronze duplicates. Chained two LEFT JOINs on `orders_df`. Orphan conditions: `col("customer_id").isNotNull() & col("cust_valid_id").isNull()` and equivalent for products. Dropped `cust_valid_id` and `prod_valid_id` before writing. The `isNotNull()` guard on the FK side correctly excluded NULL FKs from this check.

**WHAT I ACCEPTED:**
- The `.distinct()` on parent table PKs before joining was critical and something I might have missed. `bronze_customers` has 10 duplicate `customer_id` rows. Without `.distinct()`, a LEFT JOIN from orders to customers would produce multiple matching rows for each order belonging to a duplicated customer — inflating the orders row count and corrupting all downstream Silver tables. The AI caught this without prompting.
- Aliasing the parent PK columns (`cust_valid_id`, `prod_valid_id`) before the join prevents column name ambiguity — both `orders_df` and `customers_df` have a `customer_id` column, so Spark would throw an `AnalysisException` without the alias.
- The explicit `isNotNull()` guard before flagging orphans was the right call. If `customer_id` is NULL, the LEFT JOIN will return NULL on the customer side anyway — but that NULL shouldn't be flagged as a referential integrity failure. It's a completeness failure. These are separate quality dimensions and shouldn't overlap.

**WHAT I CHANGED:**
- First version chained the joins as `orders_df.join(customers, ...).join(products, ...)` but referenced `orders_df["product_id"]` in the second join condition while the first join result was named `orders_with_cust`. Spark's column resolution across chained joins can get ambiguous this way. Rewrote the second join condition to reference `col("product_id")` without the DataFrame qualifier, which works cleanly since `product_id` only comes from `orders_df` and isn't present in the customers table.
- The quality report validation section initially counted orphans using `.filter(col("quality_check_result") == "FAIL - ORPHAN customer_id")` — an exact match. This breaks for rows that fail both joins because the result is `"FAIL - ORPHAN customer_id, FAIL - ORPHAN product_id"`. Changed to `.contains("ORPHAN customer_id")` and `.contains("ORPHAN product_id")` so dual-failure rows are counted in both metrics.

**WHAT I REJECTED:**
- The first attempt used an INNER JOIN to find matching rows and then subtracted from the total to find orphans (`total - matched = orphans`). This approach is logically equivalent but falls apart when FKs can be NULL — NULL rows would be excluded from the INNER JOIN count but should also be excluded from the orphan count. The LEFT JOIN + NULL-check approach handles NULLs correctly and is more explicit. Reverted to LEFT JOIN.

**FINAL DECISION:**
Accepted after fixing the column reference ambiguity in the second join and switching `.filter(col == "FAIL - ORPHAN ...")` to `.contains(...)`. The `.distinct()` pattern on parent PKs before joining became a lesson I noted for the Gold layer joins too.

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
> - customer_segment must be one of Premium / Standard / Basic
> - signup_date must not be in the future
> - lifetime_value must be > 0
>
> concat_ws for multi-failure rows. expected 0 failures for both tables. same helper, same commenting style.

**AI RESPONSE SUMMARY:**
Generated `05_quality_business_logic.py` with `check_orders_business_logic` and `check_customers_business_logic`. For the total_amount check: `spark_round(col("quantity") * col("unit_price"), 2)` as the expected amount, `spark_abs(col("total_amount") - expected_amount) > 0.01` as the failure condition. Payment date logic used `.isin("Pending", "Cancelled") & col("payment_date").isNotNull()` for unexpected payment, and `(col("order_status") == "Completed") & col("payment_date").isNull()` for missing payment. Customer segment used `~col("customer_segment").isin(valid_segments)`. All conditions named as boolean variables before the `concat_ws` block.

**WHAT I ACCEPTED:**
- `spark_round()` on the expected amount before comparing with `spark_abs()` was a good call — without rounding the expected value first, floating-point arithmetic in the multiplication could produce something like `29.999999` vs `30.00`, which would trigger a false `total_amount mismatch` flag even when the data is correct.
- Naming each boolean condition (`amount_mismatch`, `unexpected_payment`, `missing_payment`, `future_order`) as variables before building the `concat_ws` block made the logic readable and easy to audit. Much better than embedding everything inline.
- `~col("customer_segment").isin(valid_segments)` using tilde for negation is the correct PySpark idiom — more readable than `col("customer_segment").isin(valid_segments) == False`.
- `current_date()` rather than `lit(date.today())` for the future-date check — `current_date()` evaluates at Spark execution time which is correct; hardcoding today's date as a Python literal would be wrong for a scheduled pipeline.

**WHAT I CHANGED:**
- The `total_amount` comparison initially didn't round the computed expected value: `spark_abs(col("total_amount") - (col("quantity") * col("unit_price"))) > 0.01`. The issue is that `col("quantity") * col("unit_price")` in Spark with `DecimalType` columns can produce more decimal places than the stored `total_amount`. Added `spark_round(..., 2)` around the computed expected value to match the precision of the stored field.
- The `lifetime_value > 0` check initially used `col("lifetime_value") <= 0`, which would flag NULLs as failures (Spark's three-valued logic makes `NULL <= 0` evaluate to NULL, not True — so NULLs would actually PASS, not FAIL). In this dataset there are no NULL `lifetime_value` rows, but corrected to `col("lifetime_value").isNotNull() & (col("lifetime_value") <= 0)` to be explicit. The completeness check would catch actual NULLs separately anyway.

**WHAT I REJECTED:**
- First version added an additional check on `order_status` values — flagging anything not in "Pending", "Completed", "Cancelled" as `"FAIL - invalid order_status"`. I'd already moved this check from type validation (script 03) to here. But actually, since the data generator guarantees only valid statuses, this check would always produce 0 failures and adds no value. More importantly, it's a domain constraint that belongs as documentation, not as a runtime check on data we generated ourselves. Removed it — kept business logic to the four meaningful checks specified.
- It suggested wrapping the `current_date()` comparison in `col("order_date").isNotNull() &` to avoid evaluating the date comparison on NULL order dates. Technically correct but redundant here — NULL `order_date` is caught by the type validation script (script 03), so by the time this context matters in `create_silver_tables.py`, we're already combining all flags. Left the simpler form since NULL `order_date` cases are handled upstream and the combined result in the final silver table will surface both failures anyway.

**FINAL DECISION:**
Accepted after adding `spark_round()` to the total_amount comparison and fixing the `lifetime_value` NULL handling. Zero failures confirmed on both tables after running.

---

## Prompt 6: Generate create_silver_tables.py (Silver orchestrator)

**PROMPT SENT:**
> [.cursorrules active] now generate src/silver/create_silver_tables.py — the silver orchestrator. this is the most complex one so read carefully.
>
> what it does: reads all the individual quality-check silver tables per entity, joins them together, and produces three final consolidated silver tables: silver_customers, silver_orders, silver_products
>
> joining logic: join on PK + ingestion_timestamp (both columns, not just PK). reason: if bronze has duplicate customer_ids, joining only on customer_id would fan out and corrupt the row count. ingestion_timestamp is the tiebreaker.
>
> after joining all quality-check tables for an entity: combine all quality_check_result columns into one final quality_check_result. if ALL individual checks = PASS → final = PASS. if ANY check fails → concatenate all failure reasons with " | " separator.
>
> customers checks to join: completeness, uniqueness, type_validation, business_logic (4 tables)
> orders checks to join: completeness, uniqueness, type_validation, referential_integrity, business_logic (5 tables)
> products: only type_validation was run — just promote it directly, no join needed
>
> write final tables to /FileStore/delta/silver/ with mode overwrite
> master quality report at the end: table | total_rows | passed_rows | failed_rows | pass_percentage
> expected: ~60 failed in silver_customers, ~400 failed in silver_orders, 0 failed in silver_products
>
> use same try/except fail-and-continue orchestrator pattern as bronze ingest_all.py
> sys.exit(1) on partial failure

**AI RESPONSE SUMMARY:**
Generated `create_silver_tables.py` with a `combine_quality_flags(spark, source_tables, join_keys, entity_label)` helper function that takes an ordered list of `(alias, delta_path)` tuples and a list of join keys. It loads the first table as base, renames its `quality_check_result` column to `qc_{alias}`, then iteratively LEFT JOINs each subsequent table (selecting only join keys + quality column from the right side, renamed to `qc_{alias}`). After all joins, it collects `qc_*` column names and builds the final combined column using `concat_ws(" | ", [when(col(c) != "PASS", col(c)).otherwise(lit(None)) for c in qc_columns])`. If `concat_ws` result is not empty string, it's used as the failure reason; otherwise, `"PASS"` is assigned. Drops all `qc_*` columns after. Separate `build_silver_customers`, `build_silver_orders`, `build_silver_products` functions for entity-specific path config. `run_pipeline()` orchestrates with try/except per entity.

**WHAT I ACCEPTED:**
- The `combine_quality_flags()` helper as a reusable function was the right abstraction. Customers and orders each have different numbers of quality-check tables (4 and 5 respectively) — hardcoding the join and merge logic per entity would have been repetitive and error-prone. The list-of-tuples `source_tables` parameter makes it easy to add or remove a quality check later.
- Joining on `[customer_id, ingestion_timestamp]` (both columns) was something I'd specified precisely because of the Bronze duplicate issue. If we join only on `customer_id` and a customer has two Bronze rows with the same ID, a single join with another quality-check table that also has two rows for that ID would produce a 2×2 = 4 row fan-out. `ingestion_timestamp` makes each Bronze row uniquely identifiable for the join.
- The `concat_ws(" | ", ...) != ""` condition to detect at least one failure was correct — `concat_ws` returns an empty string when all inputs are NULL (all PASS), not NULL itself, so checking `!= ""` is the right test.
- Products just doing a direct read-and-promote (no join) was clean. No need to run a `combine_quality_flags` call for a single source table.

**WHAT I CHANGED:**
- The first version of `combine_quality_flags` joined on `join_keys` using a simple list equality in the `join()` call. Spark interprets a list of column name strings correctly, but when both tables have the same column names (e.g., both have `customer_id` and `ingestion_timestamp`), the result DataFrame can end up with ambiguous duplicate column references. Added `.select(*right_cols)` on the right table before joining so only the join keys and quality column come from the right side — avoids any ambiguity from columns like `source_file_name` existing on both sides.
- The initial failure detection logic was `when(concat_ws(...) != lit(""), concat_ws(...)).otherwise(lit("PASS"))` — this calls `concat_ws` twice, meaning Spark evaluates it twice. Saved the expression to a Python variable first, though technically Spark's Catalyst optimizer would deduplicate it anyway. Changed for readability rather than performance.

**WHAT I REJECTED:**
- First attempt joined all quality-check tables using INNER JOIN instead of LEFT JOIN. An INNER JOIN would silently drop any row that didn't make it into one of the quality-check tables — for example, if `01_quality_completeness.py` failed to run and its Silver table is empty or missing, an INNER JOIN would produce 0 rows in the final `silver_customers` table. LEFT JOIN ensures that even if one quality-check table has missing rows, the final silver table keeps all rows from the base (completeness) table, with NULLs on the missing quality columns. A NULL `qc_*` column is treated as PASS by the `when(col(c) != "PASS", ...)` logic — which might not be ideal, but losing rows is far worse. Corrected to LEFT JOIN.
- The orchestrator initially ran all three `build_silver_*` functions sequentially with no error handling. I asked it to add the same `try/except` + fail-and-continue pattern from `ingest_all.py`, plus `sys.exit(1)` at the end. This mirrors the Bronze pattern exactly and ensures Databricks Jobs can detect partial failures.

**FINAL DECISION:**
Accepted after switching INNER to LEFT JOIN in `combine_quality_flags` and adding the try/except orchestration pattern. The `combine_quality_flags` function with the `(alias, path)` tuple list is the most reusable piece of the whole Silver layer — it would scale cleanly to additional quality checks without touching the orchestrator logic.
