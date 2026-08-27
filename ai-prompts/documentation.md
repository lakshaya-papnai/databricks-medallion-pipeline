# AI Prompt Diary — Documentation

This file tracks the back-and-forth while generating the core project documentation: requirements, architecture, schemas, and the README.

---

## Prompt 1: Project context setup (.cursorrules)

**PROMPT SENT:**
> Before we write any code I want to set up a .cursorrules file that gives cursor persistent memory of this project across all further sessions.
> 
> Project: Databricks medallion pipeline for an e-commerce company.
> Bronze -> Silver -> Gold -> Dashboard.
> 
> Schemas:
> customers.csv: customer_id (INT PK), customer_name, email, country, signup_date (DATE), customer_segment (Premium/Standard/Basic), lifetime_value (DECIMAL) — 10,000 rows
> orders.csv: order_id (INT PK), customer_id (INT FK), order_date, product_id (INT FK), quantity, unit_price, total_amount, order_status (Pending/Completed/Cancelled), payment_date (nullable) — 100,000 rows
> products.csv: product_id (INT PK), product_name, category, price, cost, stock_quantity, reorder_level — 500 rows
> 
> Intentional quality issues to add:
> Customers: 50 NULL emails, 10 duplicate customer_ids
> Orders: 100 NULL customer_ids, 200 NULL product_ids, 50 orphan customer_ids, 30 orphan product_ids, 20 duplicate order_ids
> 
> Strict rules to follow in every response:
> - Never delete bad rows, flag them using quality check result column (PASS/FAIL + reason)
> - Always add ingestion_timestamp and source_file_name to Bronze tables
> - Silver reads from Bronze Delta only, never raw CSVs
> - Gold reads from Silver Delta only
> - All DBFS paths should be: /FileStore/tables/
> - Code should be commented, readable, production-style
> - One file at a time only, unless specifically mentioned in the prompt to generate multiple files
> 
> Don't generate any code yet. Just create the .cursorrules file.

**AI RESPONSE:**
It generated `.cursorrules` with the project overview, the exact schemas, the planted issues list, the eight hard rules, and a progress tracker with markdown checkboxes.

**WHAT I KEPT:**
- The schema section and the hard rules were captured perfectly. Setting these constraints right off the bat saved me from having to argue with the AI about dropping bad rows later.
- The markdown progress tracker was a simple but effective way to keep track of what was left to do.

**WHAT I TWEAKED:**
- It forgot to add `schema.sql`, `reflection.md`, and `tool-workflow.md` to the progress tracker. So, I manually added them to the list.
- I had to manually check off the boxes in `.cursorrules` as we went along, since Cursor didn't self-edit its own rules file dynamically.

**WHAT I THREW OUT:**
- It preemptively added a folder for AI workflows that we hadn't agreed on yet. I deleted it to keep the file structure clean until we actually needed it.

**VERDICT:**
Accepted. Putting the "never delete bad rows" rule in `.cursorrules` was the best decision of this project.

---

## Prompt 2: Requirements analysis

**PROMPT SENT:**
> [.cursorrules active]
> 
> Now help me write requirements-analysis.md. Don't genrate any code.
> 
> it should cover: problem statement in my own words, functional requirements per layer, non-functional requirements (performance, maintainability, data quality thresholds), assumptions (I'm on Free Edition using DBFS not S3), edge cases to handle, acceptance criteria per layer.

**AI RESPONSE:**
Generated the doc. Covered all six sections, including functional specs, data quality checks, handling schema drift, and clear acceptance criteria.

**WHAT I KEPT:**
- The layer-by-layer functional requirements were spot on.
- The edge cases section was great. It proactively mentioned using `mergeSchema` for schema drift and `PERMISSIVE` mode for corrupt raw files. 

**WHAT I TWEAKED:**
- Under NFRs, it talked about using Databricks Auto Loader for incremental ingestion. I changed it to a simple batch overwrite strategy instead for this assessment.
- The acceptance criteria mentioned querying via a Databricks SQL endpoint, which CE also doesn't have. Swapped that out.

**WHAT I THREW OUT:**
- It confidently wrote a whole paragraph about using Unity Catalog for governance. Unity Catalog requires a premium workspace. Ripped it out completely to keep the requirements grounded in reality.

**VERDICT:**
Accepted after scrubbing out the enterprise features.

---

## Prompt 3: Refinement — clarify Community Edition constraints

**PROMPT SENT:**
> The requirements doc looks good overall but the assumptions section is a bit vague. Can you add a specific subsection listing exactly what architectural constraints affect this project:
> - Using DBFS instead of S3
> - No Auto Loader (batch overwrite instead)
> - Delta Lake is available (comes with runtime 13.3 LTS)
> 
> Just update the assumptions section, don't touch anything else.

**AI RESPONSE:**
Added a clean, bulleted list of architectural constraints under the Assumptions header.

**WHAT I KEPT:**
- The list was exactly what I asked for. It's important to document why certain architectural choices (like no Auto Loader) were made.
- Mentioning the 13.3 LTS runtime was a good call, as that dictates the Spark/Delta versions.
- Note: Initially I was testing locally, that's why the paths in `.cursorrules` are set to `/FileStore/tables/`. Later, I moved to Databricks Free Edition, and updated the paths to use Unity Catalog.

**WHAT I TWEAKED:**
- Nothing.

**WHAT I THREW OUT:**
- It tried to also rewrite the NFRs section again, even though I specifically said "don't touch anything else". I discarded that part of the diff and only applied the assumptions update.

**VERDICT:**
Accepted the target update, rejected the hallucinated extra edits.

---

## Prompt 4: Data quality strategy

**PROMPT SENT:**
> [.cursorrules active]
> 
> Write data-quality-strategy.md only. No code yet.
> 
> Cover all 4 checks in detail:
> Check 1 Completeness: NULL check on email (customers), customer_id and product_id (orders). Threshold >99%. Flag: FAIL - NULL email etc.
> Check 2 Uniqueness: ROW_NUMBER() window function partitioned by PK. Threshold 100% unique. Flag: FAIL - DUPLICATE order_id.
> Check 3 Referential Integrity: LEFT JOIN orders to customers and products. NULL on join side means orphan. Threshold >99.9%. Flag: FAIL - ORPHAN customer_id.
> Check 4 Type Validation: TRY_CAST for dates, >= 0 for numerics. Threshold 100%. Flag: FAIL - INVALID date or FAIL - NEGATIVE value.
> 
> Also show what the quality metrics report will look like: table with check_name, total_rows, passed_rows, failed_rows, pass_percentage. Pre-populate with our expected counts.

**AI RESPONSE:**
Generated the data quality strategy doc, detailing all four checks and providing a markdown table pre-populated with the exact failure counts we planted in the data generator.

**WHAT I KEPT:**
- The descriptions of the checks were technically accurate, specifically calling out `ROW_NUMBER()` and `TRY_CAST`.
- The metrics table was correct and matched the data generator perfectly.
- It heavily reinforced the "flag, don't delete" rule throughout the doc.

**WHAT I TWEAKED:**
- The uniqueness threshold said "100% unique", but our planted test data makes the pass rate 99.90%. I added a note clarifying that 100% is the *production* target, but during this assessment, the lower pass rate is the expected, successful outcome.

**WHAT I THREW OUT:**
- It hallucinated a fifth "Business Rules" check for email regex validation. Not part of the requirements. Ripped it out.

**VERDICT:**
Accepted after removing the extra check and clarifying the threshold logic.

---

## Prompt 5: Data model

**PROMPT SENT:**
> [.cursorrules active]
> 
> Write data-model.md only. No code.
> 
> Show schemas across all layers using markdown tables:
> 1. Source layer: 3 tables with PKs and FKs shown
> 2. Bronze: same schemas + ingestion_timestamp and source_file_name
> 3. Silver: same as Bronze + quality_check_result column
> 4. Gold: 3 aggregation tables with exact column names: gold_sales_by_product, gold_revenue_by_customer, gold_customer_segmentation. Segmentation logic: High-Value = top 20% by revenue, Repeat = 2+ completed orders, One-Time = 1 order, Inactive = 0 completed orders
> 5. Data lineage summary table: CSV -> Bronze -> Silver -> Gold
> 
> One markdown table per layer per entity. Show data types and key types.

**AI RESPONSE:**
Generated the schema reference doc, showing how the columns evolve from Source to Bronze to Silver. Drafted the Gold tables based on the aggregation logic.

**WHAT I KEPT:**
- The progressive layout (showing how `ingestion_timestamp` and `quality_check_result` get bolted on at different stages) makes it really easy to read.
- The lineage table was a nice, concise summary.

**WHAT I TWEAKED:**
- It forgot to include the `total_revenue` column in the `gold_customer_segmentation` table, which is required by the brief. I added it back.

**WHAT I THREW OUT:**
- It invented a `gold_order_details` denormalized fact table. I didn't ask for that, and we don't need it for the dashboard. Ripped it out.

**VERDICT:**
Accepted after fixing the missing column and deleting the hallucinated table.

---

## Prompt 6: Tool workflow document (Part A)

**PROMPT SENT:**
> [.cursorrules active]
> 
> Write tool-workflow.md only. Should be specific, not generic. It should read like a real engineer explaining their actual working method , not just a template.
> 
> Cover all 11 sections:
> 1. Primary tool: Cursor, why it fits
> 2. How I provide context: .cursorrules approach
> 3. AI for requirement analysis
> 4. AI for pipeline design Bronze/Silver/Gold
> 5. AI for code generation Python/PySpark/SQL
> 6. How I validate AI-generated code before accepting
> 7. AI for testing and validation
> 8. AI for debugging
> 9. What I avoid sharing with AI
> 10. How I'd reuse this workflow in production
> 11. Lessons learned — specific things AI got wrong, how I caught them

**AI RESPONSE:**
Generated the workflow doc in first-person. It gave a surprisingly good example in Section 11 about how Cursor initially tried to use `df.dropna()` in the Silver layer, which would have violated the core requirement.

**WHAT I KEPT:**
- Sections 1-9 were solid and covered exactly what the rubric asked for.
- The `df.dropna()` example in Section 11 was perfect—a real, technical example of AI making a dangerous assumption that a human had to catch.

**WHAT I TWEAKED:**
- I heavily rewrote Section 11. The AI used super corporate phrasing like "significantly accelerated the scaffolding and boilerplate generation phases". I changed it to sound like me: "made the repetitive parts much faster, like writing the same ingestion pattern three times."
- I added more emphasis on `.cursorrules` being the MVP of the project setup.

**WHAT I THREW OUT:**
- Section 10 was completely unusable. It started rambling about "enterprise data mesh architectures" and "federated governance models". I deleted all that nonsense and wrote practical examples: sharing `.cursorrules` with a team, saving prompt templates, and strictly generating one file at a time so the AI doesn't spiral out of control.

**VERDICT:**
Accepted after aggressively humanizing the tone and removing the "enterprise data mesh" buzzwords.

---

## Prompt 7: README.md

**PROMPT SENT:**
> [.cursorrules active]
> 
> Write README.md, the main setup guide for the repo. everyone should be able to clone this and run the full pipeline end to end just by following this document.
> 
> Cover:
> 1. Project overview (3-4 lines)
> 2. Architecture diagram : showing CSV -> Bronze -> Silver -> Gold -> Dashboard with notes on each layer
> 3. Full repo structure having one-line description per file
> 4. Prerequisites table
> 5. Setup steps numbered: Clone repo, pip install + run generate_sample_data.py locally, Upload CSVs to DBFS/Unity Catalog, Create cluster (13.3 LTS runtime, single node)or Serverless, Run Bronze, Run Silver, Run Gold, Set up SQL dashboard
> 6. Data quality summary table
> 7. One paragraph on AI tool usage
> 8. Project status checklist

**AI RESPONSE:**
Generated a massive, comprehensive README covering all steps, including an ASCII architecture diagram and expected output logs for each layer.

**WHAT I KEPT:**
- The ASCII diagram was clean and readable.
- It surprisingly included the exact `CREATE TABLE` SQL statements needed to register the Gold tables for the dashboard in Step 5. Huge time saver.
- Putting the "expected output" blocks under each run step is a great touch so the user knows if it actually worked.

**WHAT I TWEAKED:**
- The repo structure initially showed the Gold files as `.sql`. I had to add a callout explaining that we deliberately used `.py` instead, because Databricks SQL files can't easily write to Delta tables without PySpark context.
- Added a warning under the cluster setup step: Free Edition clusters auto-terminate after 2 hours. If they walk away, they lose their metastore and have to re-register the tables.

**WHAT I THREW OUT:**
- It added a massive, generic "Troubleshooting" section at the bottom covering standard PySpark errors. We already have a dedicated `debugging.md` file for the *actual* problems we hit, so I deleted the generic junk.

**VERDICT:**
Accepted after tweaking the file extensions, adding the cluster warning, and deleting the filler troubleshooting section.