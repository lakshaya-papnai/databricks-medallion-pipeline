import importlib
import sys
import os
import time
from datetime import datetime

# ---------------------------------------------------------
# 1. Path Setup
# ---------------------------------------------------------
# Add this script's directory to the Python path so that the
# sibling modules (whose names start with numbers) can be
# imported dynamically via importlib.
sys.path.append(os.getcwd())

# ---------------------------------------------------------
# 2. Pipeline Configuration
# ---------------------------------------------------------
# Each entry defines:
#   display_name   — human-readable label for logging
#   module_name    — importlib module to call (matches filename without .py)
#   tables_created — list of Gold Delta tables this script produces
PIPELINE = [
    {
        "display_name":   "Sales by Product",
        "module_name":    "01_sales_by_product",
        "tables_created": [
            "workspace.default.gold_sales_by_product"
        ]
    },
    {
        "display_name":   "Revenue by Customer",
        "module_name":    "02_revenue_by_customer",
        "tables_created": [
            "workspace.default.gold_revenue_by_customer"
        ]
    },
    {
        "display_name":   "Daily and Weekly Trends",
        "module_name":    "03_daily_weekly_trends",
        "tables_created": [
            "workspace.default.gold_daily_trends",
            "workspace.default.gold_weekly_trends"
        ]
    },
    {
        "display_name":   "Customer Segmentation",
        "module_name":    "04_customer_segmentation",
        "tables_created": [
            "workspace.default.gold_customer_segmentation",
            "workspace.default.gold_customer_segment_detail"
        ]
    }
]

# Full inventory of all Gold tables this pipeline creates
GOLD_TABLE_INVENTORY = [
    "workspace.default.gold_sales_by_product",
    "workspace.default.gold_revenue_by_customer",
    "workspace.default.gold_daily_trends",
    "workspace.default.gold_weekly_trends",
    "workspace.default.gold_customer_segmentation",
    "workspace.default.gold_customer_segment_detail"
]


def run_pipeline():
    """
    Orchestrates all Gold aggregation scripts in the correct order.
    Captures per-script timing and status.
    Continues past any individual failure and reports a final summary.
    """
    # ---------------------------------------------------------
    # 3. Pipeline Start
    # ---------------------------------------------------------
    pipeline_start    = time.time()
    pipeline_start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 72)
    print(f"  GOLD TABLE CREATION PIPELINE STARTED")
    print(f"  Start time: {pipeline_start_ts}")
    print("=" * 72)

    results = []

    # ---------------------------------------------------------
    # 4. Execute Each Gold Script
    # ---------------------------------------------------------
    for step in PIPELINE:
        display_name   = step["display_name"]
        module_name    = step["module_name"]
        tables_created = step["tables_created"]

        print(f"\n>>> Running: {display_name} ({module_name}.py)")
        script_start = time.time()
        status = "SUCCESS"
        error  = None

        try:
            # Dynamically import the module by name.
            # Necessary because Python cannot import number-prefixed module names
            # with standard 'import' syntax.
            module = importlib.import_module(module_name)

            # Call the module's main() function — all logic lives there.
            # This orchestrator adds NO Gold logic of its own.
            module.main()

        except Exception as e:
            # Catch any error, mark as FAILED, and continue to the next script.
            # A single failure must not block the rest of the pipeline.
            status = "FAILED"
            error  = str(e)
            print(f"[ERROR] {display_name} failed: {e}")

        script_duration = round(time.time() - script_start, 2)

        results.append({
            "display_name":   display_name,
            "module_name":    module_name,
            "status":         status,
            "duration":       script_duration,
            "tables_created": tables_created,
            "error":          error
        })

        print(f"<<< {display_name}: {status} in {script_duration}s")

    # ---------------------------------------------------------
    # 5. Master Summary Table
    # ---------------------------------------------------------
    pipeline_duration = round(time.time() - pipeline_start, 2)
    all_succeeded     = all(r["status"] == "SUCCESS" for r in results)
    overall_status    = "ALL SUCCEEDED" if all_succeeded else "PARTIAL FAILURE"

    print("\n" + "=" * 72)
    print("  GOLD TABLE CREATION PIPELINE — MASTER SUMMARY")
    print("=" * 72)
    print(f"  Overall Status : {overall_status}")
    print(f"  Total Duration : {pipeline_duration}s")
    print(f"  Start Time     : {pipeline_start_ts}")
    print(f"  End Time       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 72)
    print(f"  {'Script':<28} {'Status':<10} {'Duration':>10}  {'Gold Tables Created'}")
    print("-" * 72)

    for r in results:
        tables_str = ", ".join([t.split("/")[-1] for t in r["tables_created"]])
        print(f"  {r['display_name']:<28} {r['status']:<10} {str(r['duration']) + 's':>10}  {tables_str}")

    # Print error details below the table for easy debugging
    failed = [r for r in results if r["status"] == "FAILED"]
    if failed:
        print("\n  FAILURE DETAILS:")
        for r in failed:
            print(f"  [{r['display_name']}] {r['error']}")

    # ---------------------------------------------------------
    # 6. Final Gold Delta Table Inventory
    # ---------------------------------------------------------
    print("\n" + "=" * 72)
    print("  GOLD LAYER — FINAL TABLE INVENTORY")
    print("=" * 72)
    for path in GOLD_TABLE_INVENTORY:
        table_name = path.split("/")[-1]
        print(f"  {'✓' if all_succeeded else '?'} {table_name:<40} → {path}")
    print("=" * 72)

    # ---------------------------------------------------------
    # 7. Exit with Non-Zero Code on Failure
    # ---------------------------------------------------------
    # Non-zero exit causes Databricks Jobs to mark the run as failed,
    # enabling configured alerts and retry policies to trigger.
    if not all_succeeded:
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
