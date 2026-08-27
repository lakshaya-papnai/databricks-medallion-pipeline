from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat_ws, abs as spark_abs, current_date, round as spark_round
)

def print_quality_report(df, check_name):
    """
    Helper: prints a formatted quality metrics report for a given DataFrame.
    Shows total rows, passed rows, failed rows, and pass percentage.
    """
    total  = df.count()
    passed = df.filter(col("quality_check_result") == "PASS").count()
    failed = total - passed
    pct    = round((passed / total) * 100, 2) if total > 0 else 0.0

    print(f"\n{'─' * 60}")
    print(f"  Quality Report: {check_name}")
    print(f"{'─' * 60}")
    print(f"  {'check_name':<34} {'total':>8} {'passed':>8} {'failed':>8} {'pass_%':>8}")
    print(f"  {'-'*34} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    print(f"  {check_name:<34} {total:>8} {passed:>8} {failed:>8} {pct:>7}%")
    print(f"{'─' * 60}\n")


def check_orders_business_logic(spark, bronze_path, silver_path):
    """
    Validates business logic rules for orders:
      1. total_amount ~= quantity * unit_price  (tolerance ±0.01 for rounding)
      2. payment_date must be NULL for Pending/Cancelled orders
      3. payment_date must NOT be NULL for Completed orders
      4. order_date must not be in the future

    Reads from Bronze Delta only. Flags rows without deleting them.
    """
    print("Processing: silver_orders_business_logic")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    df = spark.table(bronze_path)

    # --- Condition 1: total_amount mismatch ---
    # Compute expected amount and compare with actual.
    # Allow a tolerance of 0.01 to accommodate floating-point rounding.
    expected_amount = spark_round(col("quantity") * col("unit_price"), 2)
    amount_mismatch = spark_abs(col("total_amount") - expected_amount) > 0.01

    # --- Condition 2: Unexpected payment_date ---
    # Pending or Cancelled orders must have NULL payment_date
    unexpected_payment = (
        col("order_status").isin("Pending", "Cancelled") & col("payment_date").isNotNull()
    )

    # --- Condition 3: Missing payment_date ---
    # Completed orders must have a non-NULL payment_date
    missing_payment = (
        (col("order_status") == "Completed") & col("payment_date").isNull()
    )

    # --- Condition 4: Future order_date ---
    # Order dates should never be in the future
    future_order = col("order_date") > current_date()

    # Build composite failure reason — concat_ws drops any None entries automatically
    flagged_df = df.withColumn(
        "quality_check_result",
        when(
            amount_mismatch | unexpected_payment | missing_payment | future_order,
            concat_ws(", ",
                when(amount_mismatch,       lit("FAIL - total_amount mismatch")).otherwise(lit(None)),
                when(unexpected_payment,    lit("FAIL - unexpected payment_date")).otherwise(lit(None)),
                when(missing_payment,       lit("FAIL - missing payment_date for completed order")).otherwise(lit(None)),
                when(future_order,          lit("FAIL - future order_date")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    flagged_df.write.format("delta").mode("overwrite").saveAsTable(silver_path)

    result_df = spark.table(silver_path)
    print_quality_report(result_df, "Business Logic - Orders")

    failed = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Failed rows: {failed}  (expected: 0)")


def check_customers_business_logic(spark, bronze_path, silver_path):
    """
    Validates business logic rules for customers:
      1. customer_segment must be one of: Premium, Standard, Basic
      2. signup_date must not be in the future
      3. lifetime_value must be > 0

    Reads from Bronze Delta only. Flags rows without deleting them.
    """
    print("Processing: silver_customers_business_logic")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    df = spark.table(bronze_path)

    # Valid segment values based on the agreed domain definition
    valid_segments = ["Premium", "Standard", "Basic"]

    # --- Condition 1: Invalid customer_segment ---
    invalid_segment = ~col("customer_segment").isin(valid_segments)

    # --- Condition 2: Future signup_date ---
    future_signup = col("signup_date") > current_date()

    # --- Condition 3: Non-positive lifetime_value ---
    # lifetime_value must be strictly greater than 0 to represent meaningful spend
    non_positive_ltv = col("lifetime_value") <= 0

    flagged_df = df.withColumn(
        "quality_check_result",
        when(
            invalid_segment | future_signup | non_positive_ltv,
            concat_ws(", ",
                when(invalid_segment,   lit("FAIL - invalid customer_segment")).otherwise(lit(None)),
                when(future_signup,     lit("FAIL - future signup_date")).otherwise(lit(None)),
                when(non_positive_ltv,  lit("FAIL - non-positive lifetime_value")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    flagged_df.write.format("delta").mode("overwrite").saveAsTable(silver_path)

    result_df = spark.table(silver_path)
    print_quality_report(result_df, "Business Logic - Customers")

    failed = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Failed rows: {failed}  (expected: 0)")


def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Silver - Business Logic Checks") \
        \
        .getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    bronze_orders_path    = "workspace.default.bronze_orders"
    bronze_customers_path = "workspace.default.bronze_customers"

    silver_orders_path    = "workspace.default.silver_orders_business_logic"
    silver_customers_path = "workspace.default.silver_customers_business_logic"

    # ---------------------------------------------------------
    # 3. Run Business Logic Checks
    # ---------------------------------------------------------
    check_orders_business_logic(spark, bronze_orders_path, silver_orders_path)
    check_customers_business_logic(spark, bronze_customers_path, silver_customers_path)

    print("Business logic checks complete. Silver tables written successfully.")

if __name__ == "__main__":
    main()
