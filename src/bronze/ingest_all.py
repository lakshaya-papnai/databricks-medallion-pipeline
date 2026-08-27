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
# imported dynamically via importlib without a package prefix.
sys.path.append(os.getcwd())

# ---------------------------------------------------------
# 2. Pipeline Configuration
# ---------------------------------------------------------
# Define the ingestion pipeline as an ordered list of tuples:
# (display_name, module_name)
# Order matters: products must run before orders because
# orders.product_id references products.product_id.
PIPELINE = [
    ("Ingest Customers", "01_ingest_customers"),
    ("Ingest Products",  "03_ingest_products"),
    ("Ingest Orders",    "02_ingest_orders"),
]

def run_pipeline():
    """
    Orchestrator: runs each Bronze ingestion script in sequence.
    Captures per-script timing, status, and row counts.
    Continues past any individual failure and reports a final summary.
    """
    # ---------------------------------------------------------
    # 3. Pipeline Start
    # ---------------------------------------------------------
    pipeline_start = time.time()
    pipeline_start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("=" * 60)
    print(f"  BRONZE INGESTION PIPELINE STARTED")
    print(f"  Start time: {pipeline_start_ts}")
    print("=" * 60)

    # Results list — one dict entry per script
    results = []

    # ---------------------------------------------------------
    # 4. Execute Each Ingestion Script
    # ---------------------------------------------------------
    for display_name, module_name in PIPELINE:
        print(f"\n>>> Running: {display_name} ({module_name}.py)")
        script_start = time.time()
        status  = "SUCCESS"
        error   = None

        try:
            # Dynamically import the module by name.
            # This is necessary because Python module names cannot
            # start with a number using a normal 'import' statement.
            module = importlib.import_module(module_name)

            # Call the module's main() function which contains all
            # ingestion logic. This script adds NO ingestion logic of its own.
            module.main()

        except Exception as e:
            # Catch any exception, mark as FAILED, and continue.
            # We do NOT re-raise — a single script failure must not
            # block the remaining scripts in the pipeline.
            status = "FAILED"
            error  = str(e)
            print(f"[ERROR] {display_name} failed: {e}")

        script_duration = round(time.time() - script_start, 2)

        results.append({
            "script":   display_name,
            "module":   module_name,
            "status":   status,
            "duration": script_duration,
            "error":    error
        })

        print(f"<<< {display_name}: {status} in {script_duration}s")

    # ---------------------------------------------------------
    # 5. Master Summary Report
    # ---------------------------------------------------------
    pipeline_duration = round(time.time() - pipeline_start, 2)
    all_succeeded     = all(r["status"] == "SUCCESS" for r in results)
    overall_status    = "ALL SUCCEEDED" if all_succeeded else "PARTIAL FAILURE"

    print("\n" + "=" * 60)
    print("  BRONZE INGESTION PIPELINE — MASTER SUMMARY")
    print("=" * 60)
    print(f"  Overall Status : {overall_status}")
    print(f"  Total Duration : {pipeline_duration}s")
    print(f"  Start Time     : {pipeline_start_ts}")
    print(f"  End Time       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    print(f"  {'Script':<22} {'Module':<26} {'Status':<10} {'Duration':>10}")
    print("-" * 60)

    for r in results:
        print(f"  {r['script']:<22} {r['module']:<26} {r['status']:<10} {str(r['duration']) + 's':>10}")

    # Print any error details below the table for easy debugging
    failed = [r for r in results if r["status"] == "FAILED"]
    if failed:
        print("\n  FAILURE DETAILS:")
        for r in failed:
            print(f"  [{r['script']}] {r['error']}")

    print("=" * 60)

    # ---------------------------------------------------------
    # 6. Exit with Non-Zero Code on Failure
    # ---------------------------------------------------------
    # When run as a Databricks Job, a non-zero exit code marks
    # the job run as failed, triggering alerts and retries if configured.
    if not all_succeeded:
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
