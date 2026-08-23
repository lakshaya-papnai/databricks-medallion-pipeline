# AI Tool Workflow

## 1. Primary AI Tool
For this assessment, I utilized Cursor as my primary AI coding assistant, specifically using the Claude Sonnet 4.6 model. Cursor is exceptionally well-suited for a data engineering workflow because it provides deep contextual awareness of the local workspace. Its ability to maintain multi-turn context and generate high-quality PySpark and SQL code makes it an ideal pair-programming partner for implementing a Databricks Medallion architecture.

## 2. Providing Project Context via `.cursorrules`
The cornerstone of my AI workflow was a `.cursorrules` file placed in the project root. Cursor automatically loads this at the start of every session, giving the AI full project context without me needing to paste anything manually. This file contained:
*   The full data schemas and intentional quality issues (e.g., 50 NULL emails, 20 duplicate order IDs).
*   Hard architectural rules (e.g., Bronze must never clean data, Silver must never delete rows, Gold must use PASS rows only).
*   DBFS path structures and exact file generation limits (one file per prompt).

This guaranteed the AI knew exactly what had been built, what was pending, and the architectural rules it must follow — automatically, from the very first prompt in every session.

## 3. Iterative Code Generation Strategy
My strategy for code generation was highly controlled and iterative:
*   **One file at a time:** I never asked the AI to generate multiple files or an entire folder structure in a single prompt. This limits hallucination and keeps the code focused and reviewable.
*   **Highly specific prompts:** Prompts always included the exact file name, its specific purpose, the schemas involved, and the expected output format. I referenced the active `.cursorrules` instead of repasting context.
*   **Rejecting and Refining:** I actively pushed back on AI suggestions that violated architectural principles. For example, in the Silver layer, the AI initially attempted to use `dropDuplicates()` for uniqueness checks and `INNER JOIN`s in the orchestrator. I rejected both, instructing it to use `ROW_NUMBER()` to flag duplicates and `LEFT JOIN`s to preserve all rows, strictly adhering to the "never delete bad rows" rule.

## 4. Validating AI-Generated Code
Before accepting any generated code into the codebase, I performed a strict review against my own criteria:
*   **Rule compliance:** Did it follow the hard rules from `.cursorrules`? (e.g., using `mode("overwrite")` instead of `dbutils.fs.rm`, and avoiding `FAILFAST` options on Bronze reads).
*   **Layer boundaries:** Did it read and write from the correct layers? (e.g., Gold reading from Silver Delta, not Bronze or raw CSV).
*   **Technical accuracy:** I caught and corrected subtle logical errors, such as the AI using `NTILE(5)` instead of `PERCENT_RANK()` for revenue segmentation in the Gold layer, or merging weekly data across multiple years by sorting on `week_number` without `order_year`.

## 5. Testing and Validation
Because I intentionally planted specific data quality issues, I treated these planted issues as my test oracle. After each layer ran, I required the AI to generate validation sections within the scripts to read back from the newly written Delta tables and print summary metrics. For instance, in the Silver layer, I verified that exactly 50 orphan customer IDs were flagged, ensuring the `LEFT JOIN` and `isNotNull()` logic was functioning correctly.

## 6. Debugging and Troubleshooting
When a piece of code didn't behave as expected, my workflow was to analyze the Spark execution logic rather than just pasting error messages. For example:
*   When calculating total amounts in the business logic check, floating-point arithmetic caused false positives. I recognized this and instructed the AI to add `spark_round()` before comparison.
*   When joining tables with identical column names in the Silver orchestrator, the AI created ambiguous column references. I instructed it to use `.select()` to scope the right-side columns prior to the join.

I ensured I fully understood the underlying logic of every fix before applying it, never blindly copy-pasting suggestions.

## 7. Data Privacy and Security
I am extremely careful about what I share with the AI. I share no real customer PII, no production credentials, and no real database connection strings. All data used in this project was synthetically generated or mocked up. This was a deliberate choice specifically to avoid leaking any sensitive information to external LLM providers.

## 8. Lessons Learned
Overall, using Claude Sonnet 4.6 via Cursor drastically accelerated the coding process, especially for establishing structural templates (like the 7-section layout used across the Bronze scripts) and generating boilerplate PySpark syntax. 

However, the workflow highlighted that AI is an assistant, not an architect. The AI frequently defaulted to "easy" solutions that violate robust data engineering patterns—such as filtering out NULL rows instead of flagging them, using `INNER JOIN` where `LEFT JOIN` was required to prevent data loss, or attempting to cast data types in Silver that were already enforced at the Bronze boundary. 

This reinforced that while AI can write the syntax, the engineer must own the architecture, enforce the design principles, and meticulously review every line of code generated. The use of a strict `.cursorrules` file and an iterative, push-back prompt style was essential to maintaining the integrity of the Medallion architecture.
