from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

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

def check_customers_uniqueness(spark, bronze_path, silver_path):
    """
    Detects duplicate customer_ids using ROW_NUMBER().
    The FIRST occurrence of a customer_id is kept as 'PASS'.
    Any subsequent row with the same customer_id is flagged 'FAIL - DUPLICATE customer_id'.
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_customers_uniqueness")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    # Read from Bronze Delta — NEVER from raw CSV
    df = spark.read.format("delta").load(bronze_path)

    # Define a window partitioned by customer_id, ordered by ingestion_timestamp.
    # ROW_NUMBER() = 1 means first occurrence → PASS
    # ROW_NUMBER() > 1 means subsequent occurrence of same ID → FAIL
    window_spec = Window.partitionBy("customer_id").orderBy("ingestion_timestamp")

    flagged_df = df \
        .withColumn("_row_num", row_number().over(window_spec)) \
        .withColumn(
            "quality_check_result",
            when(col("_row_num") > 1, lit("FAIL - DUPLICATE customer_id"))
            .otherwise(lit("PASS"))
        ) \
        .drop("_row_num")   # Drop the helper column before writing

    # Write to Silver Delta
    flagged_df.write.format("delta").mode("overwrite").save(silver_path)

    # Read back and report
    result_df = spark.read.format("delta").load(silver_path)
    print_quality_report(result_df, "Uniqueness - Customer ID")

    dup_count = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Duplicate customer_ids flagged: {dup_count}  (expected: 10)")


def check_orders_uniqueness(spark, bronze_path, silver_path):
    """
    Detects duplicate order_ids using ROW_NUMBER().
    The FIRST occurrence of an order_id is kept as 'PASS'.
    Any subsequent row with the same order_id is flagged 'FAIL - DUPLICATE order_id'.
    Reads from Bronze Delta, writes flagged result to Silver Delta.
    """
    print("Processing: silver_orders_uniqueness")
    print(f"  Source : {bronze_path}")
    print(f"  Target : {silver_path}")

    # Read from Bronze Delta — NEVER from raw CSV
    df = spark.read.format("delta").load(bronze_path)

    # Define window partitioned by order_id, ordered by ingestion_timestamp.
    # Same logic as customers: row 1 = original, row 2+ = duplicate.
    window_spec = Window.partitionBy("order_id").orderBy("ingestion_timestamp")

    flagged_df = df \
        .withColumn("_row_num", row_number().over(window_spec)) \
        .withColumn(
            "quality_check_result",
            when(col("_row_num") > 1, lit("FAIL - DUPLICATE order_id"))
            .otherwise(lit("PASS"))
        ) \
        .drop("_row_num")   # Drop the helper column before writing

    # Write to Silver Delta
    flagged_df.write.format("delta").mode("overwrite").save(silver_path)

    # Read back and report
    result_df = spark.read.format("delta").load(silver_path)
    print_quality_report(result_df, "Uniqueness - Order ID")

    dup_count = result_df.filter(col("quality_check_result") != "PASS").count()
    print(f"  Duplicate order_ids flagged: {dup_count}  (expected: 20)")


def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder.appName("Silver - Uniqueness Checks").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    bronze_customers_path = "output/delta/bronze/bronze_customers"
    bronze_orders_path    = "output/delta/bronze/bronze_orders"

    silver_customers_path = "output/delta/silver/silver_customers_uniqueness"
    silver_orders_path    = "output/delta/silver/silver_orders_uniqueness"

    # ---------------------------------------------------------
    # 3. Run Uniqueness Checks
    # ---------------------------------------------------------
    check_customers_uniqueness(spark, bronze_customers_path, silver_customers_path)
    check_orders_uniqueness(spark, bronze_orders_path, silver_orders_path)

    print("Uniqueness checks complete. Silver tables written successfully.")

if __name__ == "__main__":
    main()
