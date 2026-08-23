from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws

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


def check_referential_integrity(spark, orders_path, customers_path, products_path, silver_path):
    """
    Validates that every non-NULL customer_id and product_id in orders
    exists in the respective parent (dimension) table.

    Strategy:
      1. LEFT JOIN orders → customers on customer_id.
         If the customer side is NULL but the order's customer_id is NOT NULL,
         the order has an orphan customer_id.
      2. LEFT JOIN orders → products on product_id.
         Same logic for orphan product_ids.
      3. Rows with NULL customer_id / product_id are deliberately EXCLUDED from
         this check — they are already caught by the Completeness check (file 01).
      4. A row can fail both joins simultaneously — concat_ws() handles that.

    Reads from Bronze Delta only. Never deletes rows.
    """
    print("Processing: silver_orders_referential_integrity")
    print(f"  Orders (child)   : {orders_path}")
    print(f"  Customers (parent): {customers_path}")
    print(f"  Products (parent) : {products_path}")
    print(f"  Target           : {silver_path}")

    # Read all three Bronze Delta tables
    orders_df    = spark.read.format("delta").load(orders_path)
    customers_df = spark.read.format("delta").load(customers_path)
    products_df  = spark.read.format("delta").load(products_path)

    # Keep only the PK columns from parent tables for the join.
    # Using distinct() ensures we are comparing against unique IDs only.
    valid_customers = customers_df.select(
        col("customer_id").alias("cust_valid_id")
    ).distinct()

    valid_products = products_df.select(
        col("product_id").alias("prod_valid_id")
    ).distinct()

    # --- Join 1: Detect orphan customer_ids ---
    # LEFT JOIN orders to customers. A NULL on valid_customers side
    # (where order's customer_id is NOT NULL) signals an orphan.
    orders_with_cust = orders_df.join(
        valid_customers,
        orders_df["customer_id"] == valid_customers["cust_valid_id"],
        how="left"
    )

    # --- Join 2: Detect orphan product_ids ---
    # LEFT JOIN the result above to products, same logic.
    orders_with_both = orders_with_cust.join(
        valid_products,
        orders_df["product_id"] == valid_products["prod_valid_id"],
        how="left"
    )

    # --- Build quality_check_result ---
    # Conditions:
    #   orphan_customer = customer_id is NOT NULL but join returned no match
    #   orphan_product  = product_id is NOT NULL but join returned no match
    # Rows with NULL customer_id / product_id are handled by completeness — PASS here.
    orphan_customer_flag = (
        col("customer_id").isNotNull() & col("cust_valid_id").isNull()
    )
    orphan_product_flag = (
        col("product_id").isNotNull() & col("prod_valid_id").isNull()
    )

    flagged_df = orders_with_both.withColumn(
        "quality_check_result",
        when(
            orphan_customer_flag | orphan_product_flag,
            concat_ws(", ",
                when(orphan_customer_flag, lit("FAIL - ORPHAN customer_id")).otherwise(lit(None)),
                when(orphan_product_flag,  lit("FAIL - ORPHAN product_id")).otherwise(lit(None))
            )
        )
        .otherwise(lit("PASS"))
    )

    # Drop the helper join columns before writing
    flagged_df = flagged_df.drop("cust_valid_id", "prod_valid_id")

    # Write to Silver Delta
    flagged_df.write.format("delta").mode("overwrite").save(silver_path)

    # Read back and report
    result_df = spark.read.format("delta").load(silver_path)
    print_quality_report(result_df, "Referential Integrity - Orders")

    # Granular expected-vs-actual counts
    orphan_cust = result_df.filter(
        col("quality_check_result").contains("ORPHAN customer_id")
    ).count()
    orphan_prod = result_df.filter(
        col("quality_check_result").contains("ORPHAN product_id")
    ).count()

    print(f"  Orphan customer_ids flagged: {orphan_cust}  (expected: 50)")
    print(f"  Orphan product_ids flagged:  {orphan_prod}  (expected: 30)")


def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Silver - Referential Integrity Checks") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    bronze_orders_path    = "output/delta/bronze/bronze_orders"
    bronze_customers_path = "output/delta/bronze/bronze_customers"
    bronze_products_path  = "output/delta/bronze/bronze_products"

    silver_orders_path    = "output/delta/silver/silver_orders_referential_integrity"

    # ---------------------------------------------------------
    # 3. Run Referential Integrity Check
    # ---------------------------------------------------------
    check_referential_integrity(
        spark,
        orders_path    = bronze_orders_path,
        customers_path = bronze_customers_path,
        products_path  = bronze_products_path,
        silver_path    = silver_orders_path
    )

    print("Referential integrity check complete. Silver table written successfully.")

if __name__ == "__main__":
    main()
