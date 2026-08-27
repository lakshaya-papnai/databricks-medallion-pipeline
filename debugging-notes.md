# Debugging Notes

This file covers the actual headaches I hit during pipeline development and deployment. 

---

## Phase 1: Local Development

### Issue 1: Spark version mismatch throwing Java errors
**Layer:** Pipeline Execution Setup
**Script:** `run_local_pipeline.sh`

**What happened:**
Every Bronze script crashed immediately with `java.lang.NoSuchMethodError: 'scala.collection.Seq org.apache.spark.sql.types.StructType.toAttributes()'`. 

**Why it happened:**
The machine I was using had a global Spark 3.5.0 installed at `/opt/bigdata/spark`. The virtual environment was supposed to use PySpark 3.4.0 (which is compatible with `delta-spark==2.4.0`). The system path was picking up the global Spark instead of the venv one, and 3.5.0 broke the Delta libraries.

**How I fixed it:**
I had to update `run_local_pipeline.sh` to explicitly export `SPARK_HOME` and update the `PATH` so it forced the system to look inside the venv first before trying to run anything.

**AI involvement:**
I pasted the Java stack trace to the AI. It immediately recognized it as a Delta/Spark version conflict and told me to check `which pyspark`. Once we saw it was hitting `/opt/bigdata/`, we knew we had to force the path.

---

### Issue 2: Sed failed to update multi-line Spark sessions
**Layer:** Silver & Gold
**Script:** Terminal `sed` commands

**What happened:**
I ran a `sed` command to inject the local Delta configurations into all 13 Python scripts. But when I ran the pipeline, half of them failed because they didn't have the Delta config.

**Why it happened:**
My `sed` regex assumed `SparkSession.builder` was all on one line. Several of the scripts used `\` line continuations to break the builder across multiple lines. `sed` just skipped those completely.

**How I fixed it:**
I ran `grep -r "DeltaSparkSessionExtension" src/ | wc -l` and saw it only updated 7 out of 13 files. Instead of writing a crazier regex, I just gave the AI the list of the 6 missing files and told it to manually fix the builder blocks.

**AI involvement:**
I gave it the filenames, it did the targeted find-and-replace.

---

### Issue 3: DBFS paths hardcoded everywhere
**Layer:** All Layers
**Script:** `src/**/*.py`

**What happened:**
The pipeline couldn't find any files locally. It kept complaining about `/FileStore/...` missing.

**Why it happened:**
Because the prompt originally targeted Databricks, the AI hardcoded absolute DBFS paths (like `/FileStore/tables/` and `/FileStore/delta/`) into every single script. Those obviously don't exist on my local Linux file system.

**How I fixed it:**
I used a global find-and-replace to swap `/FileStore/tables/` for a local `data/` folder, and `/FileStore/delta/` for `output/delta/`. I made sure to exclude `dashboard_queries.sql` and the dashboard guide, since those *do* need to run in Databricks.

**AI involvement:**
I had the AI write and run the find-and-replace script to ensure we didn't miss any obscure paths.

---

### Issue 4: Silver join fan-out (silent data issue)
**Layer:** Silver
**Script:** `src/silver/create_silver_tables.py`

**What happened:**
The row counts in the final `pipeline_run.log` were too high. `silver_customers` had 10,150 rows (should be ~10,010) and `silver_orders` had 100,620 (should be ~100,020). The scripts didn't crash, the data just quietly inflated.

**Why it happened:**
The Silver orchestrator uses a `LEFT JOIN` to combine the results of multiple quality checks. If a bad row fails *two* distinct checks (e.g., missing ID and negative amount), it appears in two separate failure tables. The `LEFT JOIN` hits it twice, duplicating the row in the final Silver table.

**How I fixed it:**
I didn't. I left it as a known limitation for this specific assessment. The Gold layer aggressively filters `WHERE quality_check_result = 'PASS'`, so all these duplicated failure rows just get dumped anyway. It doesn't impact the final dashboard numbers at all. 

**AI involvement:**
I asked the AI why the counts were off, and it traced the fan-out back to the `LEFT JOIN` logic. In a real prod setup, we'd rewrite this to use a single-pass flagging system instead of joining separate result tables.

---

### Issue 5: WindowExec performance warning
**Layer:** Gold
**Script:** `src/gold/04_customer_segmentation.py`

**What happened:**
During the Gold layer run, Spark spammed the console with: `WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.`

**Why it happened:**
In script 04, we use `PERCENT_RANK()` to find the top 20% of customers by revenue. Because we want a global rank, we can't use a `PARTITION BY` clause. Spark has to shove all the data onto a single node to calculate the global rank.

**How I fixed it:**
Ignored it. We're running 10,000 customers on a local machine, so a single partition takes less than a second. 

**AI involvement:**
The AI saw the warning in the logs and explained exactly what it meant. We noted that if this was a billion-row table in a real Databricks cluster, this would be a fatal bottleneck and we'd have to use approximate percentiles instead.

---

## Phase 2: Databricks Free Edition Deployment

### Issue 6: Pathing incompatibility with Unity Catalog
**Layer:** All layers
**Error message:** `Path does not exist: data/customers.csv` and `Invalid output path: output/delta/`
**Root cause:** Local relative paths (`data/`) and local output directories (`output/delta/`) are not valid in Databricks Unity Catalog.
**How I found it:** The Bronze ingestion script failed immediately when trying to read the local CSV on the cloud.
**Fix applied:** Wrote a bulk refactor script (`refactor.py`) to change all CSV reads to Unity Catalog Volume paths (`/Volumes/workspace/default/raw_data/`), change all Delta paths to managed tables (`workspace.default.<table_name>`), convert `.save()` to `.saveAsTable()`, and change `.load()` to `spark.table()`.
**AI involvement:** AI wrote the automated refactor script to crawl all 14 files and apply the regex replacements instantly.
**Outcome:** Scripts successfully routed data to the correct Unity Catalog locations on the Free Edition.

### Issue 7: NameError: name '__file__' is not defined
**Layer:** Bronze and Gold Orchestrators
**Error message:** `NameError: name '__file__' is not defined`
**Root cause:** Databricks Serverless executes scripts as `spark_python_tasks`. In this dynamic execution context, the `__file__` variable does not exist.
**How I found it:** The pipeline crashed at line 13 of `ingest_all.py` when attempting to dynamically import sibling modules.
**Fix applied:** Replaced `os.path.dirname(__file__)` with `os.getcwd()`. Databricks Serverless automatically sets the working directory to the folder containing the executed script.
**AI involvement:** AI diagnosed the Databricks-specific context error and provided the `os.getcwd()` fix.
**Outcome:** Orchestrator scripts successfully mapped their paths and imported sibling modules.

### Issue 8: Spark Connect and Local Master Conflict
**Layer:** Bronze
**Error message:** `[CANNOT_CONFIGURE_SPARK_CONNECT_MASTER] Spark Connect server and Spark master cannot be configured together: Spark master [local[*]], Spark Connect...`
**Root cause:** The local scripts explicitly initialized the SparkSession with `.master("local[*]")`. Databricks Serverless uses Spark Connect. You cannot configure both simultaneously.
**How I found it:** Immediate crash upon `SparkSession.builder.getOrCreate()` execution on Serverless compute.
**Fix applied:** Created `fix_spark_session.py` to crawl all 13 Python files and strip `.master("local[*]")` from the builder chains.
**AI involvement:** AI identified the backend conflict between Serverless compute and local master definitions and provided the cleanup script.
**Outcome:** Spark sessions initialized natively on Databricks Serverless without conflict.

### Issue 9: Open-source Delta Config Conflict
**Layer:** All Layers
**Error message:** Proactively caught before execution.
**Root cause:** Scripts contained `.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")` and `.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")`. These override Databricks' optimized, built-in Delta engine, causing write failures in Unity Catalog.
**How I found it:** Audited the `SparkSession` builders after the previous master conflict issue.
**Fix applied:** Ran `fix_configs.py` to systematically delete these two configuration lines from all scripts.
**AI involvement:** AI proactively highlighted the risk of overriding Databricks Delta defaults and wrote the removal script.
**Outcome:** Scripts utilized the native Databricks Delta engine compatible with Unity Catalog.

### Issue 10: Silver Orchestration Gap
**Layer:** Silver
**Error message:** `[TABLE_OR_VIEW_NOT_FOUND] The table or view workspace.default.silver_customers_completeness cannot be found.`
**Root cause:** Locally, a bash script (`run_local_pipeline.sh`) executed the 5 individual Silver quality check scripts before running `create_silver_tables.py` to join them. In the Databricks Job, only `create_silver_tables.py` was executed, so the intermediate quality tables were never created.
**How I found it:** The Silver task in the Databricks Workflow failed instantly when trying to join the missing tables.
**Fix applied:** Refactored `create_silver_tables.py` from a passive joiner script into a full autonomous orchestrator. Injected an `importlib` execution loop at the top of the file to run the 5 quality check modules internally before combining their outputs.
**AI involvement:** AI diagnosed the orchestration gap and provided the structural refactor to inject the `importlib` loop.
**Outcome:** The Silver layer successfully generated the intermediate tables and merged them into the final Silver tables in a single Databricks task.
