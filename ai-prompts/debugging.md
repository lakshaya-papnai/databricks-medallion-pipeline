# AI Prompt Diary — Debugging Sessions

This file tracks the prompts used to resolve errors and unexpected behavior during both local development and the Databricks migration.

---

## Phase 1: Local Development Debugging

**PROMPT SENT:**
> hey, my run_local_pipeline.sh is failing on the very first script. here's the error:
> java.lang.NoSuchMethodError: 'scala.collection.Seq org.apache.spark.sql.types.StructType.toAttributes()'
> what is happening? I installed pyspark 3.4.0 in my venv.

**AI RESPONSE:** 
Identified this as a classic mismatch between the PySpark version and the Delta Lake version. Asked me to run `which pyspark` and `spark-submit --version` to see if my system path was picking up a global Spark installation instead of the virtual environment one.

**WHAT I ACCEPTED:** 
It was exactly that. I accepted its suggestion to explicitly `export SPARK_HOME` and update the `PATH` inside my bash script so it forces the use of the isolated venv Spark.

---

**PROMPT SENT:**
> my sed command `sed -i 's/SparkSession.builder.appName(.*).getOrCreate()/SparkSession.builder.appName(\1).config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()/g' src/**/*.py` didn't work on all files. it only caught 7 of them. why?

**AI RESPONSE:**
Explained that `sed` operates line-by-line by default, so it missed any files where `SparkSession.builder` was broken across multiple lines using the `\` continuation character.

**WHAT I ACCEPTED:**
Instead of fighting with multi-line regex in `sed`, I just gave the AI the list of the 6 files that were missed and told it to manually fix them using file editing tools.

---

**PROMPT SENT:**
> my row counts for silver_customers and silver_orders are too high. 
> silver_customers has 10150 (expected 10010) and silver_orders has 100620 (expected 100020). 
> none of the scripts crashed. why did the data inflate?

**AI RESPONSE:**
Traced the logic in `create_silver_tables.py`. Explained that because I am `LEFT JOIN`ing multiple quality check tables on `customer_id`, any row that fails *two* different checks will appear in two separate failure tables. The `LEFT JOIN` hits both, causing row fan-out (duplication).

**WHAT I ACCEPTED:**
Understood the explanation but chose not to change the code. Since the Gold layer filters for `quality_check_result = 'PASS'`, these duplicated failure rows are safely excluded from the final aggregations anyway.

---

## Phase 2: Databricks Free Edition Serverless Migration

**PROMPT SENT:**
> moving my pipeline to databricks serverless and unity catalog. all my paths are hardcoded like data/customers.csv and output/delta/bronze. need a way to change them to volume /Volumes/workspace/default/raw_data/ and managed tables workspace.default.table_name across all 14 files without opening them manually.

**AI RESPONSE:** 
Provided a python script `refactor.py` that used string replacement and `glob` to swap paths, change `.save()` to `.saveAsTable()`, and `.load()` to `spark.table()`.

**WHAT I ACCEPTED:** 
Accepted the script and ran it locally before zipping the src folder for upload.

---

**PROMPT SENT:**
> ran it on databricks job but got NameError: name '__file__' is not defined at line 13 of ingest_all.py where it does sys.path.append(os.path.dirname(__file__)). how to fix?

**AI RESPONSE:** 
Explained that Databricks `spark_python_tasks` don't have `__file__` defined because they run dynamically on Serverless compute. Suggested replacing it with `sys.path.append(os.getcwd())`.

**WHAT I ACCEPTED:** 
Applied the fix to `ingest_all.py` and `create_gold_tables.py`.

---

**PROMPT SENT:**
> hit another crash: [CANNOT_CONFIGURE_SPARK_CONNECT_MASTER] Spark Connect server and Spark master cannot be configured together: Spark master [local[*]], Spark Connect [unix:///databricks/sparkconnect/grpc.sock...]

**AI RESPONSE:** 
Explained that Databricks Serverless uses Spark Connect, which conflicts with the `.master("local[*]")` hardcoded in the scripts for local development. Wrote `fix_spark_session.py` to strip it out everywhere.

**WHAT I ACCEPTED:** 
Ran the script to clean up the SparkSession builders. Also accepted a proactive follow-up script to remove open-source Delta configs that clash with Unity Catalog.

---

**PROMPT SENT:**
> bronze passed but silver failed with [TABLE_OR_VIEW_NOT_FOUND] workspace.default.silver_customers_completeness cannot be found. the bash script used to run the 5 checks first but databricks is only running create_silver_tables.py

**AI RESPONSE:** 
Realised `create_silver_tables.py` lacked orchestration. Refactored the file to include an `importlib` loop at the top to execute the 5 quality checks before running the join logic.

**WHAT I ACCEPTED:** 
Accepted the structural rewrite of `create_silver_tables.py` to make it an autonomous orchestrator matching the Bronze and Gold patterns.

**FINAL OUTCOME:**
It worked finally after this last change and all tables were successfully visible in the Unity Catalog.
