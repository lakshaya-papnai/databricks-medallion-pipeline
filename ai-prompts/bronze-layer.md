# AI Prompt Diary — Bronze Layer

This file records all AI-assisted interactions related to Bronze layer ingestion scripts.

---

## Prompt 1: Generate 01_ingest_customers.py

**PROMPT SENT:**
> [.cursorrules active] ok starting the bronze layer. generate src/bronze/01_ingest_customers.py only — one file, nothing else.
>
> reads customers.csv from /FileStore/tables/customers.csv, writes to /FileStore/delta/bronze/bronze_customers as a delta table
>
> enforce this schema explicitly (do NOT infer):
> customer_id IntegerType, customer_name StringType, email StringType, country StringType, signup_date DateType, customer_segment StringType, lifetime_value DecimalType(10,2)
>
> after writing: read back from delta and print total row count, count of NULL emails, count of duplicate customer_ids (not total duplicate rows — distinct IDs that appear more than once), first 5 rows, ingestion summary at the end
>
> hard rules: no cleaning no filtering no transformations. preserve all NULLs and duplicates exactly. PySpark only. comment every section. must run as a notebook cell OR standalone script.

**AI RESPONSE SUMMARY:**
Cursor generated a clean 7-section script: Spark session init, path definitions, explicit `StructType` schema, CSV read with `header=True`, metadata columns added via `withColumn`, Delta write with `mode("overwrite")`, then validation section reading back from Delta. For the duplicate check it used `groupBy("customer_id").count().filter(col("count") > 1).count()` — which counts distinct IDs that appear more than once, not total duplicate rows. Ingestion summary printed source file, row count, timestamp pulled from the first row, NULL email count, and duplicate ID count.

**WHAT I ACCEPTED:**
- The 7-section structure with `# ---` separator comments was exactly what I wanted — clear enough to read top to bottom without knowing PySpark. Kept it as the template for the next two scripts.
- Duplicate count logic was correct. I was tempted to just do a total row count minus distinct count but that would give me the number of *extra* rows, not distinct IDs with duplicates — the AI's `groupBy().count().filter(count > 1).count()` is the right metric for spotting which IDs are problematic.
- The `if __name__ == "__main__"` guard was a good call — I hadn't asked for it but it means the script can be imported by the orchestrator without side effects, which became important in Prompt 4.
- `getOrCreate()` instead of just `builder.getOrCreate()` — works in both notebook (where spark already exists) and standalone contexts.

**WHAT I CHANGED:**
- The initial validation section printed expected counts inline as literals, e.g. `print(f"NULL emails: {null_emails} (expected: 50)")`. I removed the hardcoded expected count from the customer script because the real row count can vary if the CSV is regenerated. Felt wrong to hardcode it in the first script. (I added it back as a pattern in orders and products where it makes more sense — those have specific intentional issue counts that won't change.)
- The summary header was originally `print("=== INGESTION COMPLETE ===")` — changed to `"--- Ingestion Summary ---"` to match the visual style I wanted across all scripts.

**WHAT I REJECTED:**
- The first version used `spark.read.csv(...).option("nullValue", "")` — I told it to remove that option. Empty string and NULL are different things and I don't want Spark silently converting empty strings to NULLs during the Bronze read. Bronze is raw, full stop. The .cursorrules rule about no transformations covers this.
- It also initially added `.option("inferSchema", "false")` alongside the explicit schema. Redundant — if you pass a `schema=` argument, inferSchema is already disabled. Removed to keep it clean.

**FINAL DECISION:**
Accepted with the two small changes above. This script locked in the 7-section template and `if __name__ == "__main__"` pattern that all three Bronze ingest scripts follow.

---

## Prompt 2: Generate 02_ingest_orders.py

**PROMPT SENT:**
> [.cursorrules active] now generate src/bronze/02_ingest_orders.py — same structure as 01_ingest_customers.py, same 7 sections, same comment style
>
> paths: /FileStore/tables/orders.csv → /FileStore/delta/bronze/bronze_orders
>
> schema:
> order_id IntegerType, customer_id IntegerType, order_date DateType, product_id IntegerType, quantity IntegerType, unit_price DecimalType(10,2), total_amount DecimalType(10,2), order_status StringType, payment_date DateType
>
> important: payment_date is nullable. a lot of rows will have NULL here — pending and cancelled orders have no payment date. do NOT default it or transform it. preserve as-is.
>
> validation after writing: total row count (expected 100020), NULL customer_ids (expected 100), NULL product_ids (expected 200), duplicate order_ids (expected 20), first 5 rows, ingestion summary. print expected counts inline with actuals.

**AI RESPONSE SUMMARY:**
Generated `02_ingest_orders.py` following the same 7-section structure. Added a comment on the `payment_date` field in the schema definition explaining it's nullable by design. Validation printed actual and expected counts side by side. The comment on the Delta write mentioned "daily full-load pattern" which was a useful annotation I kept. The `payment_date` StructField comment read: `# Nullable by design — NULL for Pending and Cancelled orders`.

**WHAT I ACCEPTED:**
- The `payment_date` schema comment was a good addition — it explains *why* the field is nullable rather than leaving future readers to wonder if it was an oversight. Kept it.
- Expected counts printed inline (e.g. `(expected: 100)`) — this was actually better than what I had in `01_ingest_customers.py`. Made it standard from here.
- Column alignment in the summary print statements was readable — `f"NULL customer_ids:         {null_customers}  (expected: 100)"` — consistent spacing makes the output scannable at a glance.

**WHAT I CHANGED:**
- First version passed `nullable=False` on `order_id` in the StructField. That's fine semantically but if a row comes in with a NULL order_id (which can happen with data quality issues) Spark will silently coerce or fail rather than preserving the raw value. Changed to `nullable=True` across all fields to stay consistent with the Bronze-is-raw principle.
- The validation section initially only checked `null_customers` and `null_products` as `filter(col(...).isNull()).count()` but for `duplicate_orders` it used `count() - countDistinct()` — that gives you extra rows, not distinct problematic IDs. Flagged it and asked for the same `groupBy().count().filter(count > 1).count()` pattern used in script 01. Got it corrected immediately.

**WHAT I REJECTED:**
- The AI added a `.cache()` call on `raw_df` before the metadata columns step, reasoning that it would speed up the validation read. I removed it — Bronze writes to Delta and then reads *back from Delta* for validation (not from the in-memory DataFrame), so caching `raw_df` achieves nothing here and just wastes memory. The validation path is `spark.read.format("delta").load(bronze_path)`, completely separate from `raw_df`.

**FINAL DECISION:**
Accepted after the two corrections (nullable fields, duplicate count logic). The `(expected: X)` inline format was adopted as the standard validation pattern going forward.

---

## Prompt 3: Generate 03_ingest_products.py

**PROMPT SENT:**
> [.cursorrules active] generate src/bronze/03_ingest_products.py — same 7-section structure as the other two
>
> paths: /FileStore/tables/products.csv → /FileStore/delta/bronze/bronze_products
>
> schema: product_id IntegerType, product_name StringType, category StringType, price DecimalType(10,2), cost DecimalType(10,2), stock_quantity IntegerType, reorder_level IntegerType
>
> products has no intentional data quality issues — 500 clean rows. validation: total row count (expected 500), total NULLs across all columns (expected 0, check all columns not just specific ones), confirm cost < price for all rows, category distribution, first 5 rows, ingestion summary
>
> also add a data profiling section after the summary: min/max price, min/max cost, min/max stock_quantity — i want to sanity check the generated ranges

**AI RESPONSE SUMMARY:**
Generated `03_ingest_products.py` with the standard 7 sections plus an eighth profiling block. The NULL check iterated over all columns dynamically (`for c in validate_df.columns`) summing counts rather than checking specific fields. Profiling used a single `.agg()` call with six aggregate expressions (`spark_min`, `spark_max` aliases) collected in one `.collect()[0]` — one Spark job for all six metrics. Category distribution used `groupBy("category").count().orderBy("count", ascending=False)`.

**WHAT I ACCEPTED:**
- The dynamic NULL check (`for c in df.columns`) was better than hardcoding column names. If the schema changes, this survives without modification. Also catches the two metadata columns for free, which proved the metadata was being written correctly.
- Single `.agg()` for all profiling metrics was efficient — six separate `.count()` calls would trigger six Spark jobs. This approach uses one. Kept exactly as generated.
- The `cost_lt_price` check using `validate_df.filter(col("cost") < col("price")).count()` and comparing to `total_rows` was a neat integrity signal — if they're not equal, something is wrong with the generated data.
- `spark_min` / `spark_max` aliased on import (`from pyspark.sql.functions import min as spark_min, max as spark_max`) to avoid shadowing Python builtins. Good habit.

**WHAT I CHANGED:**
- The profiling output initially printed raw Decimal objects which show up as `Decimal('5.00')` in Python. Asked it to just use the values directly in the f-string — Spark's `.collect()[0]` returns them as Python objects and the f-string serialization is fine. Wasn't a functional issue but the output looked cleaner.
- The cost < price check initially wrote `validate_df.filter("cost < price")` using a string expression. Changed to `col("cost") < col("price")` for consistency with the rest of the script and to be explicit about column references.

**WHAT I REJECTED:**
- First attempt added a `DROP TABLE IF EXISTS` style DBFS cleanup block using `dbutils.fs.rm(bronze_path, recurse=True)` before the write, to avoid Delta log conflicts on re-run. I removed it — `mode("overwrite")` on a Delta table handles this correctly and cleanly. Using `dbutils.fs.rm` bypasses Delta's transactional write protocol and is unnecessary here. Bronze paths are always full overwrites by design.

**FINAL DECISION:**
Accepted after removing the `dbutils.fs.rm` cleanup block and fixing the string-expression filter. The profiling section using a single `.agg()` call became a pattern I referenced later when building Gold layer aggregations.

---

## Prompt 4: Generate ingest_all.py (Bronze orchestrator)

**PROMPT SENT:**
> [.cursorrules active] now generate src/bronze/ingest_all.py — this is the orchestrator that runs all three ingest scripts in sequence. does NOT duplicate any ingestion logic — just calls the main() functions from the three scripts
>
> execution order must be: customers → products → orders
> (products has to run before orders because orders references product_ids — if products delta table isn't there yet and silver runs downstream, referential integrity checks break)
>
> the three modules are named 01_ingest_customers, 02_ingest_orders, 03_ingest_products — names start with numbers so normal import won't work. use importlib to import them dynamically. use sys.path.append(os.path.dirname(__file__)) for path resolution
>
> per-script: track start + end time, status SUCCESS or FAILED
> overall: track pipeline start + end time
> if a script fails: catch the exception, print it, mark FAILED in summary, continue with remaining scripts — do NOT abort the whole pipeline
> at the end: print a master summary table (script name, module name, status, duration), then overall status: ALL SUCCEEDED or PARTIAL FAILURE
> if any script failed: exit with sys.exit(1) so databricks jobs can detect the failure

**AI RESPONSE SUMMARY:**
Generated `ingest_all.py` with the `PIPELINE` list as an ordered list of `(display_name, module_name)` tuples, `importlib.import_module(module_name)` inside a `try/except` per script, and a formatted summary table using f-string alignment with `:<22` and `:<26` column widths. `sys.exit(1)` at the end gated on `all(r["status"] == "SUCCESS" for r in results)`. Each iteration printed a `>>> Running: ...` line at start and a `<<< ScriptName: SUCCESS in Xs` line at end.

**WHAT I ACCEPTED:**
- The `PIPELINE` list of tuples at the top of the file was a clean design — the execution order is explicit and readable without burying it inside a loop. Anyone reading the file knows immediately what runs and in what sequence.
- The `importlib.import_module(module_name)` pattern was the correct way to handle number-prefixed modules. Normal `import 01_ingest_customers` is a syntax error in Python. Noted this was worth documenting in the comments, which the AI did.
- The fail-and-continue pattern per script was exactly right — catching the exception, storing it in `error`, and continuing to the next script means a single bad file doesn't block the whole pipeline. This matters in production where customers and products might succeed but orders fails due to a file issue.
- `sys.exit(1)` at the end was a good catch that I hadn't explicitly asked for in the same level of detail — it's necessary for Databricks Jobs to flag a run as failed and trigger alerts.
- The `>>> Running` / `<<< Done` progress lines made the notebook output scannable during a live run.

**WHAT I CHANGED:**
- The initial PIPELINE list had the order as customers → orders → products, which is wrong. I caught this immediately — products must load before orders because `02_ingest_orders.py` running at the Bronze level is fine on its own, but the downstream Silver layer does referential integrity checks against `bronze_products`. If products ingestion fails and orders still runs, the Silver layer will see orphaned product_ids that aren't actually orphans — they just didn't load yet. Fixed the order to customers → products → orders and added a comment explaining the dependency.
- The summary table initially only printed script name, status, and duration. I asked it to add the module name column so it's clear which physical file each row corresponds to — useful for debugging when a number-prefixed name maps to a display label.

**WHAT I REJECTED:**
- The first attempt used `__import__` instead of `importlib.import_module`. Both work for dynamic imports but `importlib.import_module` is the modern, explicit approach and is what I'd specified in the prompt. `__import__` is a lower-level function that returns the top-level package, not the module itself — it would work here since there's no package nesting, but it's confusing and non-idiomatic. Replaced with `importlib.import_module`.
- The AI initially suggested wrapping the entire `run_pipeline()` call in an outer try/except and printing a "catastrophic failure" message if something in the orchestrator itself broke. I removed it — if the orchestrator code itself is broken that's a bug, not a data failure, and I want the raw stack trace rather than a swallowed error message.

**FINAL DECISION:**
Accepted after fixing the execution order (customers → products → orders) and swapping `__import__` for `importlib.import_module`. The `importlib` + `sys.path.append(os.path.dirname(__file__))` pattern was reused identically in the Silver and Gold orchestrators.
