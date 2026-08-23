# Databricks Environment Setup Notes

**Environment:** Databricks Community Edition (free tier)
**Runtime:** 13.3 LTS (includes Apache Spark 3.4, Delta Lake 2.4)

---

## Community Edition Limitations Relevant to This Project

Understanding what Community Edition *cannot* do is as important as knowing what it can. Several standard Databricks Enterprise features were excluded from this project's design because of these limitations.

| Feature | Available in CE? | Impact on this project |
|:---|:---:|:---|
| Auto Loader (incremental file ingestion) | ❌ No | Used `mode("overwrite")` batch reads instead |
| Unity Catalog (table governance) | ❌ No | Tables registered per-session in default metastore |
| Real S3 / ADLS storage | ❌ No | Used DBFS (`/FileStore/`) as the storage layer |
| Multi-node clusters | ❌ No | Single-node cluster only — no distributed compute |
| Databricks Jobs (scheduled runs) | ❌ No | Scripts run manually in notebooks |
| Databricks SQL Warehouses (full) | ⚠️ Limited | Dashboard queries run on the cluster, not a dedicated SQL warehouse |
| Persistent cluster | ❌ No | Cluster terminates after inactivity; tables must be re-registered |

None of these limitations affect the correctness of the pipeline logic. The Bronze → Silver → Gold → Dashboard architecture, Delta table writes, quality checks, and aggregations all work identically on Community Edition — the constraints are operational (no scheduling, no enterprise storage) rather than functional.

---

## How DBFS Paths Work

DBFS (Databricks File System) is the distributed filesystem available in Community Edition. It maps to the underlying cluster storage and is accessible from any notebook on the same cluster.

### Path conventions used in this project

| Purpose | Path format | Example |
|:---|:---|:---|
| Raw CSV source files | `/FileStore/tables/<filename>` | `/FileStore/tables/customers.csv` |
| Bronze Delta tables | `/FileStore/delta/bronze/<table>` | `/FileStore/delta/bronze/bronze_customers` |
| Silver Delta tables | `/FileStore/delta/silver/<table>` | `/FileStore/delta/silver/silver_orders` |
| Gold Delta tables | `/FileStore/delta/gold/<table>` | `/FileStore/delta/gold/gold_sales_by_product` |

**Important:** `/FileStore/` is the only DBFS path that is accessible via the Databricks UI file browser and the `dbutils.fs` commands. Raw Delta table paths (outside `/FileStore/`) exist on the cluster but are not browsable through the UI. Using `/FileStore/delta/` for Delta tables keeps everything in one consistent, accessible location.

### Uploading files to DBFS

1. Go to **Data → Add Data → Upload File** in the Databricks UI
2. Files uploaded this way land at `/FileStore/tables/<filename>` automatically
3. Verify with: `dbutils.fs.ls("/FileStore/tables/")`

### Reading from DBFS in PySpark

```python
# Reading a CSV from DBFS
df = spark.read.csv("/FileStore/tables/customers.csv", header=True)

# Reading a Delta table from DBFS
df = spark.read.format("delta").load("/FileStore/delta/bronze/bronze_customers")
```

---

## How to Register Delta Tables for Databricks SQL

Delta tables written by PySpark scripts exist on DBFS as Delta files but are not automatically queryable from Databricks SQL. They must be registered in the metastore first.

Run the following in a **Databricks SQL Query Editor** or a notebook with `spark.sql()`:

```sql
-- Register a Delta table by pointing to its DBFS path
CREATE TABLE IF NOT EXISTS bronze_customers
  USING DELTA
  LOCATION '/FileStore/delta/bronze/bronze_customers';
```

**Note on Community Edition persistence:** The default metastore in Community Edition does not persist table registrations between cluster restarts. If the cluster is restarted, tables must be re-registered. For the dashboard to work after a cluster restart, re-run all `CREATE TABLE` statements from `src/dashboard/DASHBOARD_GUIDE.md` Step 1.

To avoid re-registering tables every session, create a setup notebook that runs all `CREATE TABLE` statements and run it once at the start of each session before opening the dashboard.

---

## Runtime Version: 13.3 LTS

**Why 13.3 LTS was chosen:**

- **LTS (Long Term Support)** releases are stable and do not change mid-session. Non-LTS runtimes receive more frequent updates which can introduce unexpected breaking changes.
- **13.3 LTS** includes Delta Lake 2.4 and Spark 3.4, both of which support all features used in this project: `PERCENT_RANK()`, `WEEKOFYEAR()`, `DATE_TRUNC()`, `concat_ws()`, and Delta `mode("overwrite")` writes.
- It is the most recent LTS available at the time of development on Community Edition.

To check your runtime version in a notebook:
```python
spark.version  # Returns Spark version
```

---

## Community Edition Gotchas

**1. Cluster auto-terminates after 2 hours of inactivity**
Community Edition clusters shut down automatically. Long-running pipelines should be broken into notebook cells so progress is visible. If the cluster terminates mid-run, the pipeline must be re-started from the beginning of the failed layer.

**2. Table registrations are lost on cluster restart**
See the metastore note above. Always re-run the `CREATE TABLE ... USING DELTA LOCATION ...` statements after a cluster restart before querying tables via SQL.

**3. No `dbfs:/` prefix needed in Python**
In PySpark, DBFS paths are referenced as `/FileStore/...` (no prefix). In Databricks CLI or `dbutils.fs` commands, paths use the `dbfs:/FileStore/...` prefix. Mixing these up causes `Path not found` errors.

```python
# Correct in PySpark
spark.read.format("delta").load("/FileStore/delta/bronze/bronze_customers")

# Correct in dbutils
dbutils.fs.ls("dbfs:/FileStore/delta/bronze/")
```

**4. Delta table `overwrite` recreates the schema on each run**
Using `mode("overwrite")` on a Delta table drops and recreates it. This means if column names or types change between runs, the table is fully replaced — no merge conflict, but also no history retention for schema changes. This is acceptable for a batch pipeline but would need `MERGE INTO` for a production incremental load.

**5. `%run` vs `importlib` for running scripts**
Databricks notebooks can reference other notebooks with `%run ./path/to/notebook`. However, for Python scripts with number-prefixed names (e.g., `01_ingest_customers.py`), `importlib.import_module()` is required because Python identifiers cannot start with a digit. The orchestrator scripts in this project use `importlib` for this reason.

**6. Databricks SQL requires a separate SQL warehouse or shared cluster**
On Community Edition, Databricks SQL dashboard queries run on the all-purpose cluster rather than a dedicated SQL warehouse. Ensure the cluster is running before opening the dashboard, otherwise queries will fail with a connection error.
