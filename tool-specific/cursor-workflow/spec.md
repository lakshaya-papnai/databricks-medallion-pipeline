# Cursor Workflow: Design Specification

This document summarizes the core design specifications and architectural guidelines shared with Cursor prior to code generation. These rules were derived from `design-notes.md` and `data-model.md`.

## Architecture Overview
- **Strict Medallion Architecture:** Bronze → Silver → Gold → Dashboard.
- **Hard Layer Boundaries:** Scripts in one layer are strictly prohibited from bypassing intermediate layers (e.g., Gold cannot read directly from Bronze).

## Layer-Specific Specifications

### Bronze Layer
- **Raw Ingest Only:** No data transformations, cleaning, or filtering.
- **Metadata Append:** Must append exactly 2 metadata columns: `ingestion_timestamp` and `source_file_name`.
- **Mode:** Must use batch `mode("overwrite")` for the scope of this project.

### Silver Layer
- **Quality Checks:** Must implement 5 distinct quality checks (Completeness, Uniqueness, Type Validation, Referential Integrity, Business Logic).
- **Flagging Strategy:** Must include a `quality_check_result` column. No rows are dropped.
- **Combiner Logic:** Must use `concat_ws(' | ', ...)` to concatenate multiple failure reasons on a single row.
- **Uniqueness Check:** Must use the `ROW_NUMBER()` window function to flag duplicates while passing the first occurrence.
- **Referential Integrity:** Must use `LEFT JOIN`s. Must include an `isNotNull()` guard so that rows missing a Foreign Key skip the RI failure (they are already flagged by the completeness check).

### Gold Layer
- **Data Filtering:** Must filter the Silver inputs to aggregate **PASS rows only**.
- **Customer Segmentation:** Must use `PERCENT_RANK()` (not `NTILE()`) to accurately calculate the top 20% for High-Value segmentation.
- **Join Logic:** Must use a `LEFT JOIN` in customer segmentation to ensure that Inactive customers (those with 0 orders) are successfully included in the final aggregation.
- **NULL Handling:** Must use `COALESCE(revenue, 0)` to prevent NULL revenues for customers with no completed orders.
- **Priority Categorization:** The `CASE WHEN` segmentation logic must follow a strict priority order: High-Value → Inactive → Repeat → One-Time.

## Code Structure & Paths
- **File Naming:** One file per specific check/aggregation, wrapped by an orchestrator script at each layer.
- **Path Conventions:** Must use local relative paths (`data/`, `output/delta/`) for local testing, while maintaining DBFS paths (`/FileStore/...`) strictly for Databricks-specific SQL and Markdown guides.
