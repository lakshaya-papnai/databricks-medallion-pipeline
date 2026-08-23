from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws, to_date

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
    print(f"  {'check_name':<34} {'total':>8} {'passed':>8} {'failed':>8} {'pass_%':>8}")
    print(f"  {'-'*34} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    print(f"  {check_name:<34} {total:>8} {passed:>8} {failed:>8} {pct:>7}%")
    print(f"{'─' * 60}\n")


def check_customers_types(spark, bronze_path, silver_path):
    """
    Validates customers for:
      - signup_date: must not be NULL (would indicate invalid date in source)
      - lifetime_value: must be >= 0
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_customers_type_validation")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    df = spark.read.format("delta").load(bronze_path)

    # signup_date arrives as DateType from Bronze (schema-enforced).
    # A NULL here would mean the original string couldn't be parsed.
    # lifetime_value must be a non-negative decimal.
    flagged_df = df.withColumn(
        "quality_check_result",
        when(
            col("signup_date").isNull() | (col("lifetime_value") < 0),
            concat_ws(", ",
                when(col("signup_date").isNull(),    lit("FAIL - INVALID date")).otherwise(lit(None)),
                when(col("lifetime_value") < 0,      lit("FAIL - NEGATIVE value")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    flagged_df.write.format("delta").mode("overwrite").save(silver_path)

    result_df = spark.read.format("delta").load(silver_path)
    print_quality_report(result_df, "Type Validation - Customers")

    failed = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Failed rows: {failed}  (expected: 0)")


def check_orders_types(spark, bronze_path, silver_path):
    """
    Validates orders for:
      - order_date: must not be NULL
      - quantity: must be >= 1 (an order of 0 items is invalid)
      - unit_price: must be >= 0
      - total_amount: must be >= 0
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_orders_type_validation")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    df = spark.read.format("delta").load(bronze_path)

    # Build composite failure reason string.
    # Each check contributes a reason string or None; concat_ws drops Nones.
    flagged_df = df.withColumn(
        "quality_check_result",
        when(
            col("order_date").isNull()   |
            (col("quantity") < 1)        |
            (col("unit_price") < 0)      |
            (col("total_amount") < 0),
            concat_ws(", ",
                when(col("order_date").isNull(),  lit("FAIL - INVALID date")).otherwise(lit(None)),
                when(col("quantity") < 1,         lit("FAIL - NEGATIVE value")).otherwise(lit(None)),
                when(col("unit_price") < 0,       lit("FAIL - NEGATIVE value")).otherwise(lit(None)),
                when(col("total_amount") < 0,     lit("FAIL - NEGATIVE value")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    flagged_df.write.format("delta").mode("overwrite").save(silver_path)

    result_df = spark.read.format("delta").load(silver_path)
    print_quality_report(result_df, "Type Validation - Orders")

    failed = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Failed rows: {failed}  (expected: 0)")


def check_products_types(spark, bronze_path, silver_path):
    """
    Validates products for:
      - price: must be >= 0
      - cost: must be >= 0 and must be < price (data integrity rule)
      - stock_quantity: must be >= 0
      - reorder_level: must be >= 0
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_products_type_validation")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    df = spark.read.format("delta").load(bronze_path)

    flagged_df = df.withColumn(
        "quality_check_result",
        when(
            (col("price") < 0)               |
            (col("cost") < 0)                |
            (col("cost") >= col("price"))    |   # cost must always be < price
            (col("stock_quantity") < 0)      |
            (col("reorder_level") < 0),
            concat_ws(", ",
                when(col("price") < 0,              lit("FAIL - NEGATIVE value")).otherwise(lit(None)),
                when(col("cost") < 0,               lit("FAIL - NEGATIVE value")).otherwise(lit(None)),
                when(col("cost") >= col("price"),   lit("FAIL - cost exceeds price")).otherwise(lit(None)),
                when(col("stock_quantity") < 0,     lit("FAIL - NEGATIVE value")).otherwise(lit(None)),
                when(col("reorder_level") < 0,      lit("FAIL - NEGATIVE value")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    flagged_df.write.format("delta").mode("overwrite").save(silver_path)

    result_df = spark.read.format("delta").load(silver_path)
    print_quality_report(result_df, "Type Validation - Products")

    failed = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Failed rows: {failed}  (expected: 0)")


def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder.appName("Silver - Type Validation Checks").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    bronze_customers_path = "output/delta/bronze/bronze_customers"
    bronze_orders_path    = "output/delta/bronze/bronze_orders"
    bronze_products_path  = "output/delta/bronze/bronze_products"

    silver_customers_path = "output/delta/silver/silver_customers_type_validation"
    silver_orders_path    = "output/delta/silver/silver_orders_type_validation"
    silver_products_path  = "output/delta/silver/silver_products_type_validation"

    # ---------------------------------------------------------
    # 3. Run Type Validation Checks for All Three Entities
    # ---------------------------------------------------------
    check_customers_types(spark, bronze_customers_path, silver_customers_path)
    check_orders_types(spark, bronze_orders_path,    silver_orders_path)
    check_products_types(spark, bronze_products_path,  silver_products_path)

    print("Type validation checks complete. Silver tables written successfully.")

if __name__ == "__main__":
    main()
