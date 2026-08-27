# Cursor Workflow: Task Breakdown

This document outlines how the workload was broken down into manageable, independent tasks and systematically fed to Cursor.

## 1. The "One File at a Time" Rule
- This rule was strictly enforced via `.cursorrules`.
- Multiple files were *never* requested in a single prompt. This constrained the AI's focus, reducing context loss, hallucinations, and sprawling PRs.

## 2. Strict Phase Ordering
Development proceeded in a rigid, sequential order:
1. Context Setup (`.cursorrules`, environment)
2. Planning Docs (Requirements, Design, Strategy)
3. Data Generation (Test Oracle)
4. Bronze Layer
5. Silver Layer
6. Gold Layer
7. Dashboard SQL
8. Automated Tests
9. Final Documentation

## 3. Anatomy of a Prompt
Every prompt sent to Cursor for code generation included specific, localized context:
- **Target File:** Which specific file to generate.
- **I/O Paths:** Exact input and output paths (e.g., `data/customers.csv` → `output/delta/bronze/bronze_customers`).
- **Schema Reference:** A direct reference to the schema needed for that specific file.
- **Validation Constraints:** The exact expected output counts for validation (e.g., 50 NULLs expected).
- **Hard Rules:** A reminder of which `.cursorrules` hard rules apply specifically to this file (e.g., "Remember: PASS rows only for this Gold script").

## 4. The Refinement Cycle
The workflow for every single file followed this loop:
1. **Generate:** Prompt Cursor for the file.
2. **Review:** Manually review the output against `.cursorrules` and the layer spec.
3. **Test:** Run the file locally. Validate output row counts and schema against the oracle.
4. **Commit/Correct:** If successful, commit. If failed, reject the code and prompt Cursor with the specific error or missing logic.
5. **Next File:** Move to the next task only when the current file is verified.

## 5. Specific Task Execution Examples
- **Bronze Templates:** The `01_ingest_customers.py` script was generated first and heavily reviewed. Subsequent Bronze scripts were prompted to simply "follow the exact same pattern as the customers script."
- **Silver Orchestration:** Quality check files were generated individually (`01` through `05`), tested in isolation, and only then was the `create_silver_tables.py` orchestrator generated to combine them.
- **Gold Format Correction:** When generating the Gold layer, Cursor briefly attempted to output `.sql` templates. This was caught immediately during the Review phase and corrected to `.py` (PySpark) before significant code was written.

## 6. Documenting Rejections
- Whenever AI generated incorrect logic or hallucinated requirements, the rejection was documented in the `ai-prompts/` directory along with the specific reasons (e.g., correcting architectural constraints, fixing the Silver layer compound join key).
