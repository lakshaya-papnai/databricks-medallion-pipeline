# Debugging Notes

This document captures the real issues encountered during the local pipeline development and testing phases. By bringing the Databricks Medallion architecture into a local environment, we faced several environment configuration, scaling, and architectural challenges, which are documented below for future reference.

---

## Issue 1: Version mismatch between system Spark and pip-installed Delta Lake

**Layer:** Pipeline Execution Setup
**Script:** `run_local_pipeline.sh`
**Error / Symptom:**
`java.lang.NoSuchMethodError: 'scala.collection.Seq org.apache.spark.sql.types.StructType.toAttributes()'` on every Bronze write operation.
**Root cause:**
The system had a pre-installed version of Spark (`/opt/bigdata/spark` - version 3.5.0) which was being loaded instead of the pip-installed PySpark 3.4.0 inside our virtual environment. This system version was incompatible with `delta-spark==2.4.0`.
**How I found it:**
Checked the log output and ran diagnostic commands (`which pyspark`, `echo $SPARK_HOME`) inside the virtual environment to confirm which Spark binaries were being invoked.
**Fix applied:**
Forced the `run_local_pipeline.sh` script to explicitly export `SPARK_HOME` and `PATH` to point directly to the virtual environment's PySpark installation before running Spark-dependent tasks.
**AI involvement:**
Cursor diagnosed the conflict based on the Java stack trace and suggested forcing the path.
**Outcome:**
The Bronze layer successfully wrote Delta tables without throwing the `NoSuchMethodError`.

---

## Issue 2: sed regex failed to update multi-line SparkSession builders

**Layer:** Silver & Gold Configuration
**Script:** Terminal `sed` commands
**Error / Symptom:**
Silent failure on multi-line blocks. Only 7 of 13 files updated with the required local Delta Lake configurations.
**Root cause:**
The `sed` regex pattern was designed for single-line `SparkSession.builder` calls and failed to properly match the multi-line builder chains that used line continuations (`\`).
**How I found it:**
By running `grep -r "DeltaSparkSessionExtension" src/ | wc -l`, which returned 7 instead of the expected 13.
**Fix applied:**
Manually identified the remaining missing files and directly instructed Cursor to replace the builder blocks using targeted file edits.
**AI involvement:**
The user supplied the list of missing files, and Cursor performed the targeted multi-file code replacements.
**Outcome:**
All 13 scripts correctly initialized their `SparkSession` with Delta Lake configurations.

---

## Issue 3: DBFS paths hardcoded throughout all source files

**Layer:** All Layers
**Script:** `src/**/*.py`
**Error / Symptom:**
Local pipeline runs would immediately fail complaining about missing `/FileStore/...` paths.
**Root cause:**
The project was originally written to run directly inside Databricks, which uses the Databricks File System (DBFS). These absolute paths do not exist on a local Linux machine.
**How I found it:**
Clear path not found exceptions during early local execution attempts.
**Fix applied:**
Ran a project-wide find-and-replace using `sed` to substitute `/FileStore/tables/` with `data/` (for CSVs) and `/FileStore/delta/` with `output/delta/` (for Delta tables). Intentionally excluded the dashboard SQL and `DASHBOARD_GUIDE.md` since they are exclusively run inside Databricks.
**AI involvement:**
Cursor executed the global find-and-replace operations and ensured paths were consistently updated.
**Outcome:**
The pipeline was able to read raw data and write to local Delta directories correctly.

---

## Issue 4: Silver join fan-out inflating row counts (silent data issue)

**Layer:** Silver
**Script:** `src/silver/create_silver_tables.py`
**Error / Symptom:**
The pipeline summary showed inflated row counts: `silver_customers` had 10,150 total rows (expected ~10,010), and `silver_orders` had 100,620 total rows (expected ~100,020). No explicit error was thrown.
**Root cause:**
`create_silver_tables.py` uses `LEFT JOIN`s to merge multiple individual quality check output tables. If a single row fails multiple distinct quality checks, it appears in multiple failure tables, causing the `LEFT JOIN` to duplicate the row during the merge.
**How I found it:**
By reviewing the output counts from the pipeline summary in `pipeline_run.log` and comparing them against the known test oracle numbers.
**Fix applied:**
Accepted as a known limitation for this assessment scope. The Gold layer remains completely unaffected because it aggressively filters for `quality_check_result == 'PASS'`, discarding the duplicate failure rows.
**AI involvement:**
Cursor detected and explained the root cause of the row inflation.
**Outcome:**
The limitation was acknowledged. In a production environment, this would be fixed by using a single-pass flagging approach instead of joining multiple separate result tables.

---

## Issue 5: WindowExec warning during Gold customer segmentation

**Layer:** Gold
**Script:** `src/gold/04_customer_segmentation.py`
**Error / Symptom:**
`WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.` printed repeatedly to standard output.
**Root cause:**
The `PERCENT_RANK()` window function was used without a `PARTITION BY` clause to generate a global ranking across all customers (necessary to identify the top 20% by revenue).
**How I found it:**
Spark emitted prominent warnings during the execution of the Gold layer.
**Fix applied:**
None applied. It was accepted for the local environment where processing 10,000 customers on a single node is trivial.
**AI involvement:**
Cursor identified the warning, explained why it occurred, and discussed the implications for scale.
**Outcome:**
Pipeline succeeds locally. The warning was documented as an area requiring optimization (e.g., using approximate percentile functions or partitioned ranking within pre-defined buckets) for a production deployment.

---

## Environment Notes

Running this Databricks-oriented pipeline in a local Linux environment surfaced several key architectural differences:

1. **Spark & Delta Installation:** In Databricks, Spark and Delta Lake are natively integrated in the runtime. Locally, `pyspark` and `delta-spark` must be explicitly installed and tightly version-matched. Furthermore, any pre-existing system Spark installations can interfere with virtual environments if paths are not carefully controlled.
2. **SparkSession Initialization:** Databricks automatically provides a pre-configured `spark` object. Locally, every script must manually build its `SparkSession` and explicitly pass Delta extensions and catalog classes (`io.delta.sql.DeltaSparkSessionExtension` and `org.apache.spark.sql.delta.catalog.DeltaCatalog`).
3. **File Systems (DBFS vs Local):** Databricks uses absolute DBFS paths (e.g., `/FileStore/tables/`). Local environments require relative or local absolute paths. To make scripts portable, one typically abstracts these paths behind environment variables or configuration files.
4. **Execution Model:** In Databricks, notebooks are chained via `dbutils.notebook.run()` or orchestrated via Databricks Workflows. Locally, we had to build a custom `bash` orchestrator (`run_local_pipeline.sh`) to sequence the Python module executions, manage exit codes, and handle environment variables.
5. **Compute Topology:** Warning messages regarding single-partition window operations highlight the difference between processing on a single-node local machine versus a distributed Databricks cluster where unbounded partitions become severe bottlenecks.
