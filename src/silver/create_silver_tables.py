import sys
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def print_quality_report(results):
    """
    Prints a master quality summary table across all final Silver tables.
    Format: table | total_rows | passed_rows | failed_rows | pass_percentage
    """
    print(f"\n{'=' * 72}")
    print(f"  FINAL SILVER LAYER — MASTER QUALITY REPORT")
    print(f"{'=' * 72}")
    print(f"  {'table':<30} {'total':>8} {'passed':>8} {'failed':>8} {'pass_%':>8}")
    print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for r in results:
        print(f"  {r['table']:<30} {r['total']:>8} {r['passed']:>8} {r['failed']:>8} {r['pct']:>7}%")
    print(f"{'=' * 72}\n")


def combine_quality_flags(spark, source_tables, join_keys, entity_label):
    """
    Reads multiple quality-check Silver tables and merges their
    quality_check_result columns into one final combined column.

    Logic:
      - If ALL individual checks = 'PASS' → final = 'PASS'
      - If ANY check fails → concatenate all distinct failure reasons with ' | '

    Parameters:
      source_tables : list of (alias_str, delta_path) tuples in join order
      join_keys     : list of column names to join on (e.g. customer_id + ingestion_timestamp)
      entity_label  : used only for logging

    Returns: final merged DataFrame with a single quality_check_result column
    """
    print(f"  Combining quality checks for: {entity_label}")

    # Load the first table as the base
    base_alias, base_path = source_tables[0]
    base_df = spark.read.format("delta").load(base_path)
    # Rename its quality column so it is distinguishable after joins
    base_df = base_df.withColumnRenamed("quality_check_result", f"qc_{base_alias}")

    # Iteratively LEFT JOIN each subsequent quality-check table
    for alias, path in source_tables[1:]:
        right_df = spark.read.format("delta").load(path)
        # Select only the join keys and the quality column from the right table
        right_cols = join_keys + ["quality_check_result"]
        right_df = right_df.select(*right_cols) \
                           .withColumnRenamed("quality_check_result", f"qc_{alias}")
        base_df = base_df.join(right_df, on=join_keys, how="left")

    # Collect the names of all quality columns
    qc_columns = [c for c in base_df.columns if c.startswith("qc_")]

    # Build the combined quality_check_result:
    # Use concat_ws to join failure reasons; treat 'PASS' columns as empty (None)
    # so only actual failure messages appear in the output.
    failure_parts = [
        when(col(c) != "PASS", col(c)).otherwise(lit(None))
        for c in qc_columns
    ]

    combined_df = base_df.withColumn(
        "quality_check_result",
        when(
            # Check: are ALL qc columns PASS?
            # We detect at least one failure by testing if any part is non-null
            concat_ws(" | ", *failure_parts) != "",
            concat_ws(" | ", *failure_parts)
        )
        .otherwise(lit("PASS"))
    )

    # Drop the individual qc_ helper columns — only final combined column remains
    combined_df = combined_df.drop(*qc_columns)

    return combined_df


# ---------------------------------------------------------
# Per-Entity Build Functions
# ---------------------------------------------------------

def build_silver_customers(spark):
    """
    Combines all 4 customer quality-check tables into final silver_customers.
    Quality checks: completeness, uniqueness, type_validation, business_logic.
    Writes to output/delta/silver/silver_customers.
    """
    silver_path = "output/delta/silver/silver_customers"

    source_tables = [
        ("completeness",    "output/delta/silver/silver_customers_completeness"),
        ("uniqueness",      "output/delta/silver/silver_customers_uniqueness"),
        ("type_validation", "output/delta/silver/silver_customers_type_validation"),
        ("business_logic",  "output/delta/silver/silver_customers_business_logic"),
    ]

    # Join on customer_id + ingestion_timestamp to uniquely identify each row
    join_keys = ["customer_id", "ingestion_timestamp"]

    final_df = combine_quality_flags(spark, source_tables, join_keys, "Customers")

    print(f"  Writing silver_customers → {silver_path}")
    final_df.write.format("delta").mode("overwrite").save(silver_path)

    result_df = spark.read.format("delta").load(silver_path)
    total  = result_df.count()
    passed = result_df.filter(col("quality_check_result") == "PASS").count()
    failed = total - passed
    pct    = round((passed / total) * 100, 2) if total > 0 else 0.0

    print(f"  silver_customers: {total} total | {passed} passed | {failed} failed  (expected failed: ~60)")
    return {"table": "silver_customers", "total": total, "passed": passed, "failed": failed, "pct": pct}


def build_silver_orders(spark):
    """
    Combines all 5 orders quality-check tables into final silver_orders.
    Quality checks: completeness, uniqueness, type_validation,
                    referential_integrity, business_logic.
    Writes to output/delta/silver/silver_orders.
    """
    silver_path = "output/delta/silver/silver_orders"

    source_tables = [
        ("completeness",          "output/delta/silver/silver_orders_completeness"),
        ("uniqueness",            "output/delta/silver/silver_orders_uniqueness"),
        ("type_validation",       "output/delta/silver/silver_orders_type_validation"),
        ("referential_integrity", "output/delta/silver/silver_orders_referential_integrity"),
        ("business_logic",        "output/delta/silver/silver_orders_business_logic"),
    ]

    # Join on order_id + ingestion_timestamp
    join_keys = ["order_id", "ingestion_timestamp"]

    final_df = combine_quality_flags(spark, source_tables, join_keys, "Orders")

    print(f"  Writing silver_orders → {silver_path}")
    final_df.write.format("delta").mode("overwrite").save(silver_path)

    result_df = spark.read.format("delta").load(silver_path)
    total  = result_df.count()
    passed = result_df.filter(col("quality_check_result") == "PASS").count()
    failed = total - passed
    pct    = round((passed / total) * 100, 2) if total > 0 else 0.0

    print(f"  silver_orders: {total} total | {passed} passed | {failed} failed  (expected failed: ~400)")
    return {"table": "silver_orders", "total": total, "passed": passed, "failed": failed, "pct": pct}


def build_silver_products(spark):
    """
    Products only had type_validation — promote it directly as silver_products.
    Writes to output/delta/silver/silver_products.
    """
    silver_path = "output/delta/silver/silver_products"
    source_path = "output/delta/silver/silver_products_type_validation"

    print(f"  Reading silver_products_type_validation → promoting to silver_products")

    final_df = spark.read.format("delta").load(source_path)

    print(f"  Writing silver_products → {silver_path}")
    final_df.write.format("delta").mode("overwrite").save(silver_path)

    result_df = spark.read.format("delta").load(silver_path)
    total  = result_df.count()
    passed = result_df.filter(col("quality_check_result") == "PASS").count()
    failed = total - passed
    pct    = round((passed / total) * 100, 2) if total > 0 else 0.0

    print(f"  silver_products: {total} total | {passed} passed | {failed} failed  (expected failed: 0)")
    return {"table": "silver_products", "total": total, "passed": passed, "failed": failed, "pct": pct}


# ---------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------

def run_pipeline():
    """
    Orchestrates creation of all three final Silver tables.
    Uses try/except per entity so one failure does not block the others.
    Prints a master quality report at the end.
    """
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Silver - Create Final Silver Tables") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()

    pipeline_start = time.time()
    pipeline_start_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 72)
    print(f"  SILVER TABLE CREATION PIPELINE STARTED")
    print(f"  Start time: {pipeline_start_ts}")
    print("=" * 72)

    # ---------------------------------------------------------
    # 2. Build Each Final Silver Table with Fail-and-Continue
    # ---------------------------------------------------------
    quality_results = []
    pipeline_statuses = []

    # --- Customers ---
    try:
        print("\n>>> Building: silver_customers")
        result = build_silver_customers(spark)
        quality_results.append(result)
        pipeline_statuses.append(("silver_customers", "SUCCESS", None))
        print("<<< silver_customers: SUCCESS")
    except Exception as e:
        print(f"[ERROR] silver_customers failed: {e}")
        pipeline_statuses.append(("silver_customers", "FAILED", str(e)))

    # --- Orders ---
    try:
        print("\n>>> Building: silver_orders")
        result = build_silver_orders(spark)
        quality_results.append(result)
        pipeline_statuses.append(("silver_orders", "SUCCESS", None))
        print("<<< silver_orders: SUCCESS")
    except Exception as e:
        print(f"[ERROR] silver_orders failed: {e}")
        pipeline_statuses.append(("silver_orders", "FAILED", str(e)))

    # --- Products ---
    try:
        print("\n>>> Building: silver_products")
        result = build_silver_products(spark)
        quality_results.append(result)
        pipeline_statuses.append(("silver_products", "SUCCESS", None))
        print("<<< silver_products: SUCCESS")
    except Exception as e:
        print(f"[ERROR] silver_products failed: {e}")
        pipeline_statuses.append(("silver_products", "FAILED", str(e)))

    # ---------------------------------------------------------
    # 3. Print Master Quality Report
    # ---------------------------------------------------------
    if quality_results:
        print_quality_report(quality_results)

    # ---------------------------------------------------------
    # 4. Print Pipeline Execution Summary
    # ---------------------------------------------------------
    pipeline_duration = round(time.time() - pipeline_start, 2)
    all_succeeded     = all(s == "SUCCESS" for _, s, _ in pipeline_statuses)
    overall_status    = "ALL SUCCEEDED" if all_succeeded else "PARTIAL FAILURE"

    print(f"{'=' * 72}")
    print(f"  PIPELINE STATUS : {overall_status}")
    print(f"  Total Duration  : {pipeline_duration}s")
    print(f"  End Time        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'─' * 72}")
    for table, status, error in pipeline_statuses:
        print(f"  {table:<30} {status}")
        if error:
            print(f"    Error: {error}")
    print(f"{'=' * 72}")

    # Non-zero exit on partial failure for Databricks Job alerting
    if not all_succeeded:
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()
