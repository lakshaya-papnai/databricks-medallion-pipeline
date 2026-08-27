# Candidate Information

**Name:** Lakshaya Papnai
**Role:** SE
**Primary Technology Stack:** Python, PySpark, SQL, Databricks
**Primary AI Tool Used:** Cursor (Claude Sonnet 4.6 + Gemini 3.1 Pro)
**Project Option Selected:** Data Pipeline (Medallion Architecture)
**Assessment Start Date:** 18-08-2026
**Submission Date:** 29-08-2026

## Tools & Environment
- Databricks: Free Edition
- Languages: Python, PySpark, SQL
- Libraries: PySpark, Delta Lake, pandas, faker
- AI Tool: Cursor (Claude Sonnet 4.6 + Gemini 3.1 Pro)

## Setup Summary
[Quick reference for how to run the pipeline — expanded in README.md]

1. **Local Data Generation:** Run `pip install faker pandas numpy` and execute `python src/data_generation/generate_sample_data.py` locally to create the three source CSV files.
2. **Cloud Upload:** Upload the generated CSV files and the `src/` directory to Databricks Free Edition.
3. **Execution:** Execute the pipeline orchestrator scripts sequentially on a 13.3 LTS cluster:
   - Bronze: `src/bronze/ingest_all.py`
   - Silver: `src/silver/create_silver_tables.py`
   - Gold: `src/gold/create_gold_tables.py`
4. **Dashboarding:** Use Databricks SQL to register the Gold tables and build the visualisations, or utilize Databricks Genie to automatically generate the dashboard from the `workspace.default.gold_*` tables.