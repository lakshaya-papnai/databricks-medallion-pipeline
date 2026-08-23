# AI Prompt Diary — Documentation

This file records all AI-assisted interactions for project documentation —
requirements analysis, design documents, context setup, and architectural
markdown files.

---

## Prompt 1: Project context setup — .cursorrules

**PROMPT SENT:**
> Before we write any code I want to set up a .cursorrules file that gives
> Cursor persistent memory of this project across sessions. Similar to how
> .cursorrules works in Cursor — it loads automatically so I don't have to
> re-explain the project every time.
>
> Project: Databricks medallion pipeline for an e-commerce company.
> Bronze -> Silver -> Gold -> Dashboard.
>
> Schemas:
> customers.csv: customer_id (INT PK), customer_name, email, country,
> signup_date (DATE), customer_segment (Premium/Standard/Basic),
> lifetime_value (DECIMAL) — 10,000 rows
>
> orders.csv: order_id (INT PK), customer_id (INT FK), order_date,
> product_id (INT FK), quantity, unit_price, total_amount,
> order_status (Pending/Completed/Cancelled), payment_date (nullable)
> — 100,000 rows
>
> products.csv: product_id (INT PK), product_name, category, price,
> cost, stock_quantity, reorder_level — 500 rows
>
> Intentional quality issues to plant:
> customers: 50 NULL emails, 10 duplicate customer_ids
> orders: 100 NULL customer_ids, 200 NULL product_ids,
>         50 orphan customer_ids, 30 orphan product_ids, 20 duplicate order_ids
>
> Hard rules to follow in every response:
> - Never delete bad rows — flag with quality_check_result (PASS/FAIL + reason)
> - Always add ingestion_timestamp and source_file_name to Bronze tables
> - Silver reads from Bronze Delta only, never raw CSVs
> - Gold reads from Silver Delta only
> - All DBFS paths: /FileStore/tables/
> - Code must be commented, readable, production-style
> - Tell me which file before showing code
> - One file at a time only
>
> Don't generate any code yet. Just create the .cursorrules file.

**AI RESPONSE SUMMARY:**
Generated .cursorrules with project overview, all three schemas, the full
list of intentional quality issues, eight hard rules, a to-be-generated
file list, and a progress tracker with checkboxes.

**WHAT I ACCEPTED:**
- Schema section matched column definitions exactly
- Hard rules section was complete and correctly stated
- Progress tracker format was clean and easy to update manually

**WHAT I CHANGED:**
- Added database/schema.sql, final-ai-usage-summary.md, and
  tool-workflow.md to the progress tracker — Cursor missed these
  from the required file list when generating the tracker
- Progress tracker was updated manually throughout the project
  as files were completed — Cursor cannot persist state between
  sessions so this had to be done by hand

**WHAT I REJECTED:**
- Cursor generated a tool-specific/cursor-workflow/ folder entry
  in the progress tracker before we had decided on that structure.
  Removed it from the initial tracker — added back later once
  the brief's Cursor-specific requirements were reviewed

**FINAL DECISION:**
.cursorrules created and loaded automatically by Cursor at the
start of every subsequent session.

---

## Prompt 2: Requirements analysis

**PROMPT SENT:**
> [.cursorrules active]
>
> Now help me write requirements-analysis.md. Don't generate any code.
>
> Cover: problem statement in my own words, functional requirements per
> layer, non-functional requirements (performance, maintainability,
> data quality thresholds), assumptions (I'm on Community Edition using
> DBFS not S3), edge cases to handle, acceptance criteria per layer.

**AI RESPONSE SUMMARY:**
Generated requirements-analysis.md with all six sections. Included
functional requirements per layer, NFRs covering performance and
data quality thresholds, edge cases for orphaned records, late-arriving
data, schema drift, corrupt files, and acceptance criteria per layer.

**WHAT I ACCEPTED:**
- Layer-by-layer functional requirements were accurate
- Edge cases section was thorough — schema drift via mergeSchema
  and corrupt file handling via PERMISSIVE mode were good additions
- Acceptance criteria format was measurable and clear

**WHAT I CHANGED:**
- NFR section mentioned Databricks Auto Loader for incremental
  processing. Auto Loader is not available on Community Edition.
  Changed to simple batch overwrite approach instead
- Acceptance criteria mentioned Databricks SQL endpoint for dashboard
  connections — adjusted to reflect Community Edition limitations

**WHAT I REJECTED:**
- Unity Catalog references in the governance section — Unity Catalog
  is a Premium/Enterprise feature, not available in Community Edition.
  Removed entirely to keep requirements realistic for our setup

**FINAL DECISION:**
Saved as requirements-analysis.md after removing Community Edition
incompatible features (Auto Loader, Unity Catalog, SQL endpoint).

---

## Prompt 3: Refinement — clarify Community Edition constraints

**PROMPT SENT:**
> The requirements doc looks good overall but the assumptions section
> is a bit vague. Can you add a specific subsection listing exactly
> what Community Edition can and can't do that affects this project:
> - Using DBFS instead of S3
> - No Auto Loader (batch overwrite instead)
> - No Unity Catalog
> - Single node cluster only
> - Delta Lake is available (comes with runtime 13.3 LTS)
>
> Just update the assumptions section, don't touch anything else.

**AI RESPONSE SUMMARY:**
Added a dedicated Community Edition constraints subsection under
assumptions listing all four limitations and confirming Delta Lake
availability on runtime 13.3 LTS.

**WHAT I ACCEPTED:**
- The constraints list was accurate and complete
- Noting Delta Lake availability by runtime version was a useful
  detail that fed into the README setup instructions later

**WHAT I CHANGED:**
- Nothing further needed

**WHAT I REJECTED:**
- Cursor tried to also update the NFRs section at the same time
  even though I said only update assumptions. Reverted the NFR
  changes and kept only the assumptions update

**FINAL DECISION:**
Updated assumptions section saved. NFR section left unchanged.

---

## Prompt 4: Data quality strategy

**PROMPT SENT:**
> [.cursorrules active]
>
> Write data-quality-strategy.md only. No code yet.
>
> Cover all 4 checks in detail:
>
> Check 1 Completeness: NULL check on email (customers),
> customer_id and product_id (orders). Threshold >99%.
> Flag: FAIL - NULL email etc.
>
> Check 2 Uniqueness: ROW_NUMBER() window function partitioned
> by PK. Threshold 100% unique. Flag: FAIL - DUPLICATE order_id.
>
> Check 3 Referential Integrity: LEFT JOIN orders to customers
> and products. NULL on join side means orphan. Threshold >99.9%.
> Flag: FAIL - ORPHAN customer_id.
>
> Check 4 Type Validation: TRY_CAST for dates, >= 0 for numerics.
> Threshold 100%. Flag: FAIL - INVALID date or FAIL - NEGATIVE value.
>
> Also show what the quality metrics report will look like —
> table with check_name, total_rows, passed_rows, failed_rows,
> pass_percentage. Pre-populate with our expected counts.

**AI RESPONSE SUMMARY:**
Generated data-quality-strategy.md with all four checks documented
and a pre-populated metrics report table showing expected counts
matching the planted quality issues exactly.

**WHAT I ACCEPTED:**
- All four check descriptions were accurate
- The metrics table with exact expected counts was correct
- The "never delete bad rows" principle was clearly stated as the
  core strategy throughout

**WHAT I CHANGED:**
- Uniqueness check threshold said "100% unique" but the metrics
  report showed 99.90% pass rate. Added a clarification note:
  100% is the production target. During testing, intentional
  duplicates are planted to validate the check works, so the
  report will show less than 100% by design — this is expected
  behaviour, not a failure of the check

**WHAT I REJECTED:**
- Cursor added a fifth "Business Rules" check covering email
  format regex validation and country code checks. The brief
  specifies exactly 4 checks. Removed the fifth to stay
  aligned with assessment requirements

**FINAL DECISION:**
Saved as data-quality-strategy.md with threshold clarification
note added and out-of-scope fifth check removed.

---

## Prompt 5: Data model

**PROMPT SENT:**
> [.cursorrules active]
>
> Write data-model.md only. No code.
>
> Show schemas across all layers using markdown tables:
> 1. Source layer — 3 tables with PKs and FKs shown
> 2. Bronze — same schemas + ingestion_timestamp and source_file_name
> 3. Silver — same as Bronze + quality_check_result column
> 4. Gold — 3 aggregation tables with exact column names:
>    gold_sales_by_product, gold_revenue_by_customer,
>    gold_customer_segmentation
>    Segmentation logic: High-Value = top 20% by revenue,
>    Repeat = 2+ completed orders, One-Time = 1 order,
>    Inactive = 0 completed orders
> 5. Data lineage summary table: CSV -> Bronze -> Silver -> Gold
>
> One markdown table per layer per entity. Show data types and key types.

**AI RESPONSE SUMMARY:**
Generated data-model.md with progressive schema tables showing
column additions at each layer, Gold table schemas with exact
column names and types, and a lineage summary table.

**WHAT I ACCEPTED:**
- Progressive schema layout was clear — easy to see what gets
  added at each layer
- Gold schemas matched the agreed column definitions exactly
- Lineage table was concise and useful

**WHAT I CHANGED:**
- gold_customer_segmentation was missing the total_revenue column
  in Cursor's first version. Added it back — the assessment brief
  explicitly lists it as a required column in that table

**WHAT I REJECTED:**
- Cursor added a gold_order_details denormalized fact table that
  wasn't in the brief or our design notes. Removed it — we only
  need the 3 required Gold tables plus the bonus trend tables

**FINAL DECISION:**
Saved as data-model.md with missing total_revenue column added
back and out-of-scope gold_order_details table removed.

---

## Prompt 6: Tool workflow document (Part A)

**PROMPT SENT:**
> [.cursorrules active]
>
> Write tool-workflow.md only. This is Part A, worth 20% of the score.
> First person, specific, not generic. Should read like a real engineer
> explaining their actual working method — not a template.
>
> Cover all 11 sections:
> 1. Primary tool — Cursor (Claude Sonnet 4.6), why it fits
> 2. How I provide context — .cursorrules approach, what's in it
> 3. AI for requirement analysis
> 4. AI for pipeline design Bronze/Silver/Gold
> 5. AI for code generation Python/PySpark/SQL
> 6. How I validate AI-generated code before accepting
> 7. AI for testing and validation
> 8. AI for debugging
> 9. What I avoid sharing with AI
> 10. How I'd reuse this workflow in production
> 11. Lessons learned — specific things AI got wrong, how I caught them

**AI RESPONSE SUMMARY:**
Generated tool-workflow.md in first person covering all 11 sections.
Section 11 used formal language and gave a specific example of
Cursor suggesting df.dropna() in the Silver layer which would have
deleted rows, violating the no-delete hard rule.

**WHAT I ACCEPTED:**
- Sections 1-10 covered all required points clearly
- The df.dropna() example in Section 11 was accurate and relevant —
  exactly the kind of violation the .cursorrules hard rules are
  designed to prevent

**WHAT I CHANGED:**
- Section 11 was rewritten in simpler, more natural language.
  Cursor used corporate phrasing like "significantly accelerated
  the scaffolding and boilerplate generation phases" — rewrote
  as "made the repetitive parts much faster, like writing the
  same ingestion pattern three times for three different tables"
- Added a specific mention of .cursorrules being the single most
  useful setup decision — Cursor didn't highlight this enough in
  the lessons section

**WHAT I REJECTED:**
- Section 10 (production reuse) was too abstract. Cursor wrote
  about "enterprise data mesh architectures" and "federated
  governance models" — replaced with practical specifics:
  .cursorrules becomes a team-level context file, prompt templates
  get saved per pipeline type, one-file-at-a-time pattern prevents
  Cursor from over-generating and losing control of the output

**FINAL DECISION:**
Saved as tool-workflow.md with Section 11 manually rewritten in
plain language and Section 10 replaced with concrete practical
examples instead of abstract enterprise terminology.

---

## Prompt 7: README.md

**PROMPT SENT:**
> [.cursorrules active]
>
> Write README.md — the main setup guide for the repo.
> Someone should be able to clone this and run the full pipeline
> end to end just by following this document.
>
> Cover:
> 1. Project overview (3-4 lines)
> 2. Architecture diagram — ASCII text showing CSV -> Bronze ->
>    Silver -> Gold -> Dashboard with notes on each layer
> 3. Full repo structure with one-line description per file
> 4. Prerequisites table
> 5. Setup steps numbered:
>    - Clone repo
>    - pip install + run generate_sample_data.py locally
>    - Upload CSVs to DBFS via Data -> Add Data -> Upload File
>    - Create cluster (13.3 LTS runtime, single node)
>    - Run Bronze via ingest_all.py in a notebook
>    - Run Silver via create_silver_tables.py
>    - Run Gold via create_gold_tables.py
>    - Set up SQL dashboard — include the CREATE TABLE statements
>      needed to register Delta tables in Databricks SQL
> 6. Data quality summary table (all 7 planted issues + which check catches each)
> 7. One paragraph on AI tool usage
> 8. Project status checklist

**AI RESPONSE SUMMARY:**
Generated complete README.md with all 8 sections including ASCII
architecture diagram, full repo tree with descriptions, prerequisites
table, numbered steps with expected output blocks for each layer,
data quality table, and project status checklist.

**WHAT I ACCEPTED:**
- ASCII architecture diagram was clear and accurate
- Step 8 included the CREATE TABLE SQL statements for registering
  Delta tables in Databricks SQL — useful addition not in my prompt
- Expected output blocks under each step make it easy to spot
  if something went wrong when running in Databricks

**WHAT I CHANGED:**
- Gold files shown as .sql in the repo structure (matching the
  brief template). Added a callout note explaining our Gold files
  are .py because .sql files cannot write Delta tables in
  Databricks — PySpark is required for that
- Added a warning under Step 4 about Community Edition clusters
  auto-terminating after 2 hours — users need to know to run
  the full pipeline in one session or restart before continuing

**WHAT I REJECTED:**
- Cursor added a generic Troubleshooting section at the end with
  common Spark error messages. Removed it — those errors aren't
  specific to our project and add noise. Our debugging-notes.md
  covers the real issues we actually hit during development

**FINAL DECISION:**
Saved as README.md with .py clarification added, auto-termination
warning added, and generic troubleshooting section removed.