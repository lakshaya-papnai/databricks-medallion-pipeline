# Reflection — AI-Assisted Medallion Architecture Pipeline

**Project:** E-Commerce Data Pipeline (Bronze → Silver → Gold → Dashboard)
**Tools used:** Cursor (Claude Sonnet 4.6)
**Assessment Part:** C — Submission and Reflection

---

## 1. What I Built

I built a complete Databricks medallion architecture data pipeline for an e-commerce company, processing daily sales data from three CSV sources across four layers.

**By the numbers:**

- **3 source tables** — customers (10,010 rows), orders (100,020 rows), products (500 rows)
- **3 Bronze Delta tables** — raw ingestion with metadata columns, no transformations
- **8 Silver quality-check tables** (intermediate) → **3 final Silver tables** — quality-flagged with `quality_check_result` column
- **5 Silver quality checks** — completeness, uniqueness, referential integrity, type validation, business logic
- **6 Gold Delta tables** — 4 aggregation scripts producing business-ready analytics
- **5 dashboard SQL queries** — powering bar, histogram, pie, line, and stacked bar visualisations
- **460 flagged rows** across the three Silver tables — all 7 intentional issue types caught exactly
- **21 source files** written across `src/` — all commented and production-style

Beyond the minimum requirements, I added a fifth Silver check (business logic validation — total amount arithmetic, payment date rules) and a fourth Gold table pair (daily and weekly revenue trends), neither of which was strictly required.

---

## 2. How I Used AI Across the Lifecycle

AI was not used the same way at every stage. The role shifted as the project progressed.

**Planning and design:**
I used AI first for structuring, not coding. I gave it the full business problem and schemas and asked it to produce `requirements-analysis.md`, `data-quality-strategy.md`, and `data-model.md` before a single line of code was written. This front-loading of design was deliberate — having those documents meant every subsequent prompt could reference them rather than re-explaining context. The key tool here was `.cursorrules`, a persistent context file I created early and pasted into every new session.

**Data generation:**
I used AI to write the `generate_sample_data.py` script from a detailed spec. The non-overlapping issue pool logic — using a partitioned sample of 400 indices from 100,000 to ensure no row received two planted issues — was something AI handled correctly without me having to design it explicitly. I then ran manual Python validation checks against the generated CSVs before trusting the data for the rest of the pipeline.

**Bronze layer:**
I prompted AI to generate `01_ingest_customers.py` in full detail, establishing a 7-section structure with explicit comments. For the next two Bronze scripts I simply said "follow the exact same structure" — this reuse-by-reference approach saved significant time and kept the code consistent across all three ingestion scripts.

**Silver layer:**
This was where AI's contribution was most technically meaningful. Window functions (`ROW_NUMBER()`), LEFT JOINs for referential integrity, and `concat_ws()` for building composite failure reason strings would have taken me much longer to write correctly from scratch. I gave AI the specific fields, thresholds, and expected output format, and it got the PySpark idioms right on the first attempt for most checks.

**Gold layer:**
AI handled the aggregation SQL via `spark.sql()` with temp views, which I found cleaner than chained PySpark API calls for business-level reporting. The `PERCENT_RANK()` approach for High-Value segmentation was suggested by AI in response to my spec — I had only described the desired outcome, not the mechanism.

**Dashboard:**
I gave AI the five query specifications and it generated clean, well-commented SQL. I did not change the core query logic, but I reviewed each query against the Gold table schemas in `schema.sql` to confirm column names matched exactly.

**Documentation:**
AI drafted most documents, but I rewrote or simplified in places where the output was too formal or vague. Particularly `tool-workflow.md` Section 8 (Lessons Learned) — AI wrote it in a polished, corporate voice that didn't sound like me. I rewrote that section entirely in simpler language.

---

## 3. What AI Helped With Most

**1. The referential integrity LEFT JOIN logic**
The tricky part here was not the join itself — it was the condition. Rows where `customer_id` is already NULL should not be flagged as orphans; those are caught by the completeness check. AI got this right immediately: check `customer_id IS NOT NULL` before checking whether the join produced a match. I would have likely missed that distinction on first attempt.

**2. The `concat_ws()` pattern for multi-failure rows**
The requirement was: if a single order row has both a NULL `customer_id` and a NULL `product_id`, the `quality_check_result` should read `'FAIL - NULL customer_id, NULL product_id'` — not just one reason. AI suggested using `concat_ws(", ", when(condition1, lit("reason1")).otherwise(lit(None)), when(condition2, lit("reason2")).otherwise(lit(None)))`. This pattern — where `concat_ws` silently drops NULL values — was the correct idiom and cleaner than any approach I would have reached for first.

**3. The `importlib` pattern for number-prefixed module names**
My ingestion scripts are named `01_ingest_customers.py`, `02_ingest_orders.py` — valid filenames, but invalid Python identifiers. Standard `import` statements cannot handle them. AI immediately suggested `importlib.import_module("01_ingest_customers")` combined with `sys.path.append(os.path.dirname(__file__))`. I reused this pattern identically in the Silver and Gold orchestrators.

**4. `PERCENT_RANK()` for High-Value segmentation**
I described the requirement as "top 20% of customers by revenue should be High-Value." AI suggested `PERCENT_RANK() OVER (ORDER BY total_revenue DESC)` and noted that ordering DESC means rank 0.0 is the top earner — so `percent_rank <= 0.20` captures the top 20%. I had considered `NTILE(5)` but `PERCENT_RANK` is more semantically correct here because it produces a continuous value rather than discrete buckets.

---

## 4. What AI Got Wrong or Needed Correction

I want to be honest here because this is where most of the real learning happened.

**1. Auto Loader and Unity Catalog in requirements**
The first draft of `requirements-analysis.md` included non-functional requirements about Auto Loader for incremental ingestion and Unity Catalog for governance. Both are Databricks Enterprise features — not available on Community Edition, which is the environment this project runs on. I caught this during review of the document and removed those items before moving forward. If I had not reviewed the requirements carefully, those assumptions would have produced a design that couldn't actually run in my environment.

**2. Silver orchestrator join fan-out risk**
In `create_silver_tables.py`, the `combine_quality_flags()` function joins multiple per-check Silver tables on the primary key column (e.g., `customer_id` for customers). The problem is that Bronze tables contain duplicate customer_ids (10 of them, intentionally planted). Joining on `customer_id` alone would fan out — one row would match multiple rows with the same ID in the next table, multiplying the row count incorrectly. The fix was to join on both `customer_id` AND `ingestion_timestamp`, which together uniquely identify a row. AI did not suggest this correction unprompted — I identified the risk when thinking through the join logic and specified the compound key in the prompt.

**3. Products validation was too thin initially**
The first pass at `03_ingest_products.py` followed the same validation pattern as the customers and orders scripts — print NULL counts, print duplicates. For products, both of those are expected to be zero because products has no intentional issues. The output was technically correct but not meaningful. I added a data profiling section (min/max for price, cost, stock_quantity) as an additional validation — this was my addition, not AI's. AI had simply replicated the existing pattern without considering that different tables need different validations.

**4. Uniqueness threshold phrasing**
In the data quality strategy document, AI wrote "Threshold: 100% unique" for the uniqueness check. But after running the uniqueness check against orders with 20 duplicate order_ids out of 100,020 rows, the pass percentage is actually 99.98%, not 100%. The threshold language needed to mean "any duplicate is a failure" — not that we expect 100% of rows to pass. It's a phrasing distinction, but it could confuse someone reading the strategy document who then looks at the quality report and sees 99.98%. I updated the strategy document to clarify: "threshold for acceptable data = 100% unique; pass rate will reflect actual duplicate count."

---

## 5. How I Validated AI Output

I treated validation as a non-negotiable step at each stage, not as an afterthought.

**Data generation:** Before moving to Bronze, I ran manual Python validation commands against all three CSVs, checking every single planted issue type against its exact expected count. All 460 issues verified. Only after this did I treat the CSVs as trustworthy source data.

**Bronze layer:** The validation section at the end of each Bronze script reads back from Delta (not from memory) and prints NULL counts and duplicate counts. The critical check was confirming those counts matched the input — proving that no cleaning had happened during ingestion. If the NULL count had dropped, something was wrong with the write.

**Silver layer:** I compared expected vs actual counts at the end of every Silver check script. The format `(expected: 50)` next to the actual count made discrepancies immediately visible in Databricks output. I confirmed all 7 issue types were caught with the correct counts.

**Code review against .cursorrules:** Before accepting any generated script, I read it against the hard rules in `.cursorrules` — particularly checking that Silver scripts read from Bronze Delta only (never CSVs), that no rows were deleted, and that all DBFS paths used `/FileStore/` format.

**Schema cross-check:** When writing `schema.sql`, I compared every column name and type against the actual code in the PySpark scripts to confirm they matched. This caught two minor type description mismatches in the documentation.

---

## 6. What I Would Improve Next Time

**1. Run the pipeline in Databricks earlier — much earlier**
I designed and wrote all pipeline code before running any of it in Databricks. That meant path issues, table registration requirements for the dashboard, and cluster compatibility questions would only have surfaced at the very end. In the future I would run a skeleton version of each layer in Databricks as soon as the first script was done, and fix environmental issues incrementally rather than all at once.

**2. Write validation assertions before code (test-first)**
If I had written assertions like `assert null_email_count == 50` in a separate test file before writing the Silver completeness check, the join fan-out bug in the orchestrator would have been caught immediately — the row count would not have matched. Having the expected output written down first forces you to think about what the code should do before you write it. I wrote validations after the code, which is the wrong order.

**3. Split `create_silver_tables.py` into smaller pieces**
The `combine_quality_flags()` function in the Silver orchestrator is doing a lot: iteratively joining multiple tables, renaming columns, building composite failure strings, then writing the result. It is complex to read and harder to debug. A cleaner design would be separate helper functions per entity — `combine_customer_flags()`, `combine_order_flags()` — each with their own explicit join key list and a simpler, more transparent join chain.

**4. Use MERGE instead of overwrite for incremental loads**
Every table in this pipeline uses `mode("overwrite")`, which replaces the entire table on each run. For a daily batch pipeline this works, but in production you would want Delta Lake's `MERGE INTO` operation for incremental loads — inserting new rows, updating changed rows, and preserving existing clean rows without reprocessing the full dataset. This was out of scope for Community Edition but would be the first thing I would change for a real deployment.

---

## 7. Reusable Workflow Patterns I Developed

These are patterns I would carry into any future AI-assisted data engineering project.

**`.cursorrules` as persistent context file**
Creating a structured `.cursorrules` file at the start — containing schemas, hard rules, intentional issues, and a progress tracker — meant I never had to repeat project context across sessions. Every session started with the same shared understanding because Cursor automatically injected this context. This is the single highest-value habit to adopt.

**One file at a time prompt pattern**
Explicitly instructing AI to generate one file per response prevents the temptation to request everything at once. When AI generates multiple files together without explicit permission, review quality drops because you have too much to check simultaneously. Smaller units of output = more careful review = fewer accepted errors.

**Expected vs actual counts in every validation output**
Printing `(expected: 50)` alongside the actual count in every validation script transforms the output from data you have to interpret into data that self-diagnoses. An evaluator — or a future me six months later — can read the Databricks output without having to remember what the expected numbers should be.

**Orchestrator pattern with fail-and-continue**
Separating logic scripts (`01_ingest_customers.py`) from execution scripts (`ingest_all.py`) creates a clean boundary between "what this step does" and "how the pipeline runs." The fail-and-continue pattern in each orchestrator means one broken script does not silently block the rest of the pipeline — failures surface in the summary table and trigger `sys.exit(1)` for Databricks Job alerting.

**Prompt log template**
The `ai-prompts/` diary format — PROMPT SENT → AI RESPONSE SUMMARY → WHAT I ACCEPTED → WHAT I CHANGED → WHAT I REJECTED → FINAL DECISION — forced me to be deliberate about what I was accepting from AI rather than copying code passively. That deliberateness is what makes the difference between using AI as a tool and being used by it.
