# AI Prompt Diary — Bronze Layer

This file records the chat history and decisions made while designing and generating the synthetic sample data for the pipeline.

---

## Prompt 1: Generate 01_ingest_customers.py

**PROMPT SENT:**
> [.cursorrules active] starting the bronze layer, generate src/bronze/01_ingest_customers.py only as one file, nothing else.
> 
> reads customers.csv from /FileStore/tables/customers.csv, writes to /FileStore/delta/bronze/bronze_customers as delta table
> 
> enforce this schema explicitly (do NOT infer): customer_id int, customer_name string, email string, country string, signup_date date, customer_segment string, lifetime_value decimal(10,2)
> 
> after writing: read back from delta and print total row count, count of NULL emails, count of duplicate customer_ids (distinct IDs, not total duplicate rows), first 5 rows, ingestion summary at the end
> 
> hard rules: no cleaning no filtering no transformations. preserve all NULLs and duplicates exactly. pyspark only. comment every section. must run as notebook cell OR standalone script.

**AI RESPONSE:**
It wrote a clean 7-section script: init spark, paths, explicit `StructType`, CSV read, adding metadata with `withColumn`, write to Delta with `overwrite`, and a validation read. For the duplicate check, it used `groupBy("customer_id").count().filter(col("count") > 1).count()` which perfectly counts the distinct problematic IDs instead of just total duplicate rows. Also printed the summary I asked for.

**WHAT I KEPT:**
- The 7-section layout with `# ---` comments was exactly what I wanted. Super readable. I ended up making it the template for the next two scripts.
- The duplicate count logic was solid. I was originally thinking of just doing total count minus distinct count, but that gives you the number of *extra* rows, not the number of IDs causing the problem. The AI's approach was better.
- It added an `if __name__ == "__main__"` block. I didn't ask for it, but it was a smart move for making the script importable later.

**WHAT I TWEAKED:**
- It tried to print expected counts inline like `(expected: 50)`. I actually pulled that out for the customers script because the total row count can change if we regen the CSV. Felt weird to hardcode it here. (Spoiler: I added it back for orders and products later because it was actually pretty useful).
- Changed the summary header from `=== INGESTION COMPLETE ===` to `--- Ingestion Summary ---` to match my visual style.

**WHAT I THREW OUT:**
- It threw in `.option("nullValue", "")` for the CSV read. I killed that immediately. Empty strings aren't NULLs, and Bronze is supposed to be raw data, no transformations.
- It also explicitly disabled `inferSchema` even though I provided a schema. That's redundant in Spark, so I just took it out.

**VERDICT:**
Accepted with those minor tweaks. This locked in the template for the rest of the Bronze layer.

---

## Prompt 2: Generate 02_ingest_orders.py

**PROMPT SENT:**
> [.cursorrules active] Now generate src/bronze/02_ingest_orders.py — same structure as 01_ingest_customers.py, same 7 sections, same comments
> 
> paths: /FileStore/tables/orders.csv -> /FileStore/delta/bronze/bronze_orders
> 
> schema: order_id int, customer_id int, order_date date, product_id int, quantity int, unit_price dec(10,2), total_amount dec(10,2), order_status string, payment_date date
> 
> important: payment_date is nullable. lots of rows will have NULL here (pending/cancelled orders). do NOT default it or transform it. preserve as-is.
> 
> validation after writing: total row count (expected 100020), NULL customer_ids (expected 100), NULL product_ids (expected 200), duplicate order_ids (expected 20), first 5 rows, ingestion summary. print expected counts inline with actuals.

**AI RESPONSE:**
It wrote `02_ingest_orders.py` following the 7-section template perfectly. It added a comment next to `payment_date` noting that it's nullable by design, and printed the expected counts side-by-side with the actuals in the validation section.

**WHAT I KEPT:**
- I really liked the schema comment it added for `payment_date`: `# Nullable by design — NULL for Pending and Cancelled orders`. Explains the *why* instead of just the *what*.
- Printing `(expected: X)` inline was definitely the right move. I decided to make it the standard going forward.
- It padded the print statements so all the numbers aligned visually. Little things like that make the logs way more readable.

**WHAT I TWEAKED:**
- It tried to set `nullable=False` on `order_id` in the schema. Semantically true, but if bad data comes in with a NULL order_id, Spark will crash instead of preserving the raw row. I flipped all fields to `nullable=True` just to stick to the Bronze-is-raw rule.
- For the duplicate orders check, it regressed and used `count() - countDistinct()` instead of the group-by logic from the first script. I told it to fix it and use the same logic from script 01.

**WHAT I THREW OUT:**
- It tried to add `.cache()` on the raw DataFrame before the metadata step to "speed up validation." Pointless. Validation reads back from Delta, not from the in-memory dataframe, so caching here just wastes memory. Deleted it.

**VERDICT:**
Accepted after fixing the nullability and the duplicate math. 

---

## Prompt 3: Generate 03_ingest_products.py

**PROMPT SENT:**
> [.cursorrules active] generate src/bronze/03_ingest_products.py — same 7-section structure as the others
> 
> paths: /FileStore/tables/products.csv -> /FileStore/delta/bronze/bronze_products
> 
> schema: product_id int, product_name string, category string, price dec(10,2), cost dec(10,2), stock_quantity int, reorder_level int
> 
> products has no intentional issues — 500 clean rows. validation: total row count (expected 500), total NULLs across all cols (expected 0, check all cols not just specific ones), confirm cost < price for all rows, category dist, first 5 rows, ingestion summary
> 
> also add a data profiling section after summary: min/max price, min/max cost, min/max stock_quantity — wanna sanity check the generated ranges

**AI RESPONSE:**
It wrote `03_ingest_products.py` with an extra eighth section for profiling. To check for NULLs across all columns, it dynamically looped through `validate_df.columns`. The profiling part used a single `.agg()` call to grab all six min/max metrics at once, which was smart. 

**WHAT I KEPT:**
- The dynamic loop for the NULL check was a nice touch. If the schema changes later, the check won't break, and it caught the metadata columns for free.
- Doing one `.agg()` call for all six profiling metrics was super efficient. Six separate `.count()` calls would have triggered six Spark jobs.
- The logic to verify `cost < price` by filtering for it and comparing the result to the total row count was a clever integrity check.

**WHAT I TWEAKED:**
- It wrote the cost < price filter as a string expression (`filter("cost < price")`). Changed it to `col("cost") < col("price")` just to stay consistent with the rest of the codebase.

**WHAT I THREW OUT:**
- It tried to add a block using `dbutils.fs.rm(bronze_path, recurse=True)` before the write to clean up the directory. Totally unnecessary. Writing to Delta with `mode("overwrite")` handles all of that automatically.

**VERDICT:**
Accepted after ripping out the `dbutils` cleanup code. That single `.agg()` profiling pattern actually became super useful later on in the Gold layer.

---

## Prompt 4: Generate ingest_all.py (Bronze orchestrator)

**PROMPT SENT:**
> [.cursorrules active] Now generate src/bronze/ingest_all.py — this is the orchestrator that runs all three ingest scripts in sequence. does NOT duplicate any logic ,just calls main() from the three scripts
> 
> execution order must be: customers -> products -> orders
> (products has to run before orders cause orders references product_ids — if products delta isn't there yet, silver ref integrity checks break later)
> 
> the modules are named 01_ingest_customers, 02_ingest_orders, 03_ingest_products. names start with numbers so normal import won't work. use importlib to import them dynamically. use sys.path.append(os.path.dirname(__file__)) for path resolution
> 
> per-script: track start/end time, status SUCCESS or FAILED
> overall: track pipeline start/end time
> if a script fails: catch it, print it, mark FAILED in summary, but continue with remaining scripts , do NOT abort whole pipeline
> at end: print master summary table (script name, module name, status, duration), then overall status: ALL SUCCEEDED or PARTIAL FAILURE
> if any script failed: sys.exit(1) so databricks jobs catch it

**AI RESPONSE:**
It created `ingest_all.py` with an ordered `PIPELINE` list of tuples mapping names to modules. Wrapped `importlib.import_module()` in a try/except block for each script, and set up a nicely formatted summary table at the end. It properly gated the `sys.exit(1)` based on the final statuses.

**WHAT I KEPT:**
- Defining the `PIPELINE` execution order upfront as a list was a clean design. Anyone reading the file instantly knows the run sequence.
- Using `importlib.import_module` was the right call for handling the number-prefixed python files.
- The try/except fail-and-continue pattern was exactly what I wanted. If the customers load fails, I still want products to attempt a load. 
- The console output with `>>> Running` and `<<< SUCCESS` made the logs super easy to read during execution.

**WHAT I TWEAKED:**
- The initial order it suggested was customers → orders → products. Nope. Orders needs products to exist first for the Silver layer checks to work. I fixed the order and added a comment explaining the dependency.
- The summary table only printed the script name at first. I had it add the actual module filename to the table so debugging would be easier later.

**WHAT I THREW OUT:**
- The first draft used `__import__` instead of `importlib.import_module`. Technically works, but it's old and weird. I told it to switch to the modern `importlib` approach.
- It tried to wrap the whole orchestrator in a massive outer try/except block to catch "catastrophic failures." I deleted that — if the orchestrator itself has a bug, I want the raw stack trace, not a swallowed error message.

**VERDICT:**
Accepted after fixing the run order and the import method. I ended up reusing this exact orchestrator pattern for both the Silver and Gold layers.
