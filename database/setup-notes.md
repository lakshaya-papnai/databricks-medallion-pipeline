# Databricks Environment Setup Notes

**Environment:** Databricks Free Edition
**Compute:** Serverless
**Data Governance:** Unity Catalog

---

## Unity Catalog Structure

This project bypasses legacy DBFS (`/FileStore/`) paths in favor of modern Unity Catalog governance.

### 1. Raw Data Storage (Volumes)
Raw source files (CSVs) are uploaded to a Unity Catalog Volume. Volumes are the recommended way to manage non-tabular data files in Databricks.
- **Path:** `/Volumes/workspace/default/raw_data/`
- **Usage:** Used by the Bronze layer as the source location for `.csv` reads.

### 2. Managed Delta Tables
All Delta tables created by the Bronze, Silver, and Gold layers are saved as **Managed Tables** in Unity Catalog.
- **Schema:** `workspace.default`
- **Naming convention:** `workspace.default.<layer>_<entity>` (e.g., `workspace.default.bronze_customers`)
- **Usage:** The pipeline uses `.saveAsTable("workspace.default.table_name")` instead of specifying underlying cloud object storage paths. Unity Catalog abstracts away the physical file storage location.

---

## Databricks Workflows (Job Orchestration)

The medallion pipeline is executed automatically using a Databricks Job constructed as a Directed Acyclic Graph (DAG) with three sequential Python tasks.

### Job Configuration:
- **Compute Type:** Serverless Compute (eliminates cluster startup time and master/worker configuration conflicts).
- **Source:** Workspace files (the zipped `src/` directory uploaded to the Databricks Workspace).

### Task Dependencies:
1. **Bronze Task:**
   - Script: `src/bronze/ingest_all.py`
   - Depends On: None
2. **Silver Task:**
   - Script: `src/silver/create_silver_tables.py`
   - Depends On: Bronze Task
3. **Gold Task:**
   - Script: `src/gold/create_gold_tables.py`
   - Depends On: Silver Task

This orchestrator structure ensures that if the Bronze layer fails (e.g., missing CSV), the Silver and Gold layers will not run.

---

## Table Registration & Querying

Because tables are saved directly into Unity Catalog using `saveAsTable()`, there is no need to manually run `CREATE TABLE ... USING DELTA LOCATION ...` statements to register them for SQL querying.

- **PySpark Reads:** `spark.table("workspace.default.silver_orders")`
- **SQL Reads:** `SELECT * FROM workspace.default.gold_sales_by_product`

The tables are instantly available in the Catalog Explorer and the Databricks SQL query editor the moment the PySpark job finishes.

---

## Dashboard Generation (Databricks Genie)

The final Executive Dashboard was generated using **Databricks Genie** within the Databricks SQL Persona. 

By providing Genie with a master prompt pointing to the four `workspace.default.gold_*` tables, the AI automatically interpreted the schema, calculated the KPIs, and constructed the Bar, Line, and Donut charts required for the business stakeholders. No manual SQL dashboard configuration was required.
