# Cursor Workflow: Project Context

This document explains how project context was established and maintained for Cursor throughout the development of the Databricks Medallion pipeline.

## 1. Context Initialization
- **` .cursorrules` created first:** The `.cursorrules` file was written and placed in the project root before a single line of Python code was generated. This ensured AI behavior was constrained from the very first prompt.
- **Automatic Context Loading:** Every new Cursor session automatically loaded the `.cursorrules` file, guaranteeing the AI was always aware of the overall architectural rules and the current project state.

## 2. Contents of `.cursorrules`
The rules file served as the master context document, containing:
- A high-level **project summary** and goal definition.
- **Full data schemas**, explicitly listing data types and expected row counts for every table.
- A complete list of **intentional data quality issues** (the test oracle), specifying exact counts for validation (e.g., exactly 50 NULL emails).
- **8 hard rules** that Cursor was strictly forbidden from violating.
- A comprehensive **list of files to generate**, ensuring a structured "one at a time" workflow.
- A **progress tracker**, manually updated as each file was completed and tested.

## 3. Schema Upfront, No Inference
- The complete schemas (Bronze, Silver, Gold) were provided upfront in their entirety.
- Schemas were *never* provided piecemeal. This eliminated the risk of the AI "hallucinating" or incorrectly inferring column names, data types, or relationships.

## 4. Explicit Negative Constraints
Instead of just telling Cursor what to do, the context explicitly stated what **NOT** to do at each specific layer:
- **Bronze:** *NO* cleaning, *NO* type casting, *NO* filtering. Just raw append.
- **Silver:** *NO* deleting rows. Erroneous records must be flagged, not dropped.
- **Gold:** *ONLY* aggregate rows where `quality_check_result = 'PASS'`. Do not aggregate dirty data.
