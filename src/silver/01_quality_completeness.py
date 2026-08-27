from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, lit, trim

def print_quality_report(df, check_name):
    """
    Helper: prints a formatted quality metrics report for a given DataFrame.
    Shows total rows, passed rows, failed rows, and pass percentage.
    """
    total   = df.count()
    passed  = df.filter(col("quality_check_result") == "PASS").count()
    failed  = total - passed
    pct     = round((passed / total) * 100, 2) if total > 0 else 0.0

    print(f"\n{'─' * 60}")
    print(f"  Quality Report: {check_name}")
    print(f"{'─' * 60}")
    print(f"  {'check_name':<30} {'total':>8} {'passed':>8} {'failed':>8} {'pass_%':>8}")
    print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    print(f"  {check_name:<30} {total:>8} {passed:>8} {failed:>8} {pct:>7}%")
    print(f"{'─' * 60}\n")

def check_customers_completeness(spark, bronze_path, silver_path):
    """
    Checks customers for NULL email (the only completeness check for this table).
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_customers_completeness")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    # Read from Bronze Delta — NEVER from raw CSV
    df = spark.table(bronze_path)

    # Apply completeness check:
    # If email IS NULL → FAIL, otherwise → PASS
    flagged_df = df.withColumn(
        "quality_check_result",
        when(col("email").isNull(), lit("FAIL - NULL email"))
        .otherwise(lit("PASS"))
    )

    # Write to Silver Delta — mode overwrite for idempotent daily runs
    flagged_df.write.format("delta").mode("overwrite").saveAsTable(silver_path)

    # Read back and report
    result_df = spark.table(silver_path)
    print_quality_report(result_df, "Completeness - Customer Email")

    # Validate against expected planted issues
    null_email_count = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  NULL emails flagged: {null_email_count}  (expected: 50)")


def check_orders_completeness(spark, bronze_path, silver_path):
    """
    Checks orders for NULL customer_id and NULL product_id.
    A single row can fail both checks simultaneously — the reason string
    is built by concatenating all failure messages for that row.
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_orders_completeness")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    # Read from Bronze Delta — NEVER from raw CSV
    df = spark.table(bronze_path)

    # Build a list of per-column failure flags.
    # Each when() returns the failure reason string or None.
    # concat_ws joins non-null parts with ', ' to support multi-failure rows.
    flagged_df = df.withColumn(
        "quality_check_result",
        when(
            col("customer_id").isNull() | col("product_id").isNull(),
            # Build the composite reason string for rows failing one or both checks
            concat_ws(", ",
                when(col("customer_id").isNull(), lit("FAIL - NULL customer_id")).otherwise(lit(None)),
                when(col("product_id").isNull(),  lit("FAIL - NULL product_id")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    # Write to Silver Delta
    flagged_df.write.format("delta").mode("overwrite").saveAsTable(silver_path)

    # Read back and report
    result_df = spark.table(silver_path)

    # Individual check counts for detailed validation
    null_cust  = result_df.filter(col("customer_id").isNull()).count()
    null_prod  = result_df.filter(col("product_id").isNull()).count()

    print_quality_report(result_df, "Completeness - Order FKs")
    print(f"  NULL customer_ids flagged: {null_cust}  (expected: 100)")
    print(f"  NULL product_ids flagged:  {null_prod}  (expected: 200)")


def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder.appName("Silver - Completeness Checks").getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    bronze_customers_path = "workspace.default.bronze_customers"
    bronze_orders_path    = "workspace.default.bronze_orders"

    silver_customers_path = "workspace.default.silver_customers_completeness"
    silver_orders_path    = "workspace.default.silver_orders_completeness"

    # ---------------------------------------------------------
    # 3. Run Completeness Checks
    # ---------------------------------------------------------
    check_customers_completeness(spark, bronze_customers_path, silver_customers_path)
    check_orders_completeness(spark, bronze_orders_path, silver_orders_path)

    print("Completeness checks complete. Silver tables written successfully.")

if __name__ == "__main__":
    main()
