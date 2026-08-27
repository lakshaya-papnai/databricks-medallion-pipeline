from pyspark.sql import SparkSession

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Gold - Sales by Product") \
        \
        .getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # Gold reads from final Silver tables ONLY — never from Bronze or raw CSVs
    silver_orders_path   = "workspace.default.silver_orders"
    silver_products_path = "workspace.default.silver_products"
    gold_path            = "workspace.default.gold_sales_by_product"

    # ---------------------------------------------------------
    # 3. Load Silver Tables — PASS rows only
    # ---------------------------------------------------------
    # HARD RULE: Only rows that passed all quality checks are used for aggregations.
    # This ensures bad/flagged data never contaminates Gold reporting.
    orders_df   = spark.table(silver_orders_path) \
                       .filter("quality_check_result = 'PASS'")

    products_df = spark.table(silver_products_path) \
                       .filter("quality_check_result = 'PASS'")

    # ---------------------------------------------------------
    # 4. Register Temp Views for spark.sql()
    # ---------------------------------------------------------
    # Using temp views allows us to write the aggregation as a clean SQL query,
    # which is more readable and closer to what analysts would write.
    orders_df.createOrReplaceTempView("silver_orders")
    products_df.createOrReplaceTempView("silver_products")

    # ---------------------------------------------------------
    # 5. Run Aggregation via spark.sql()
    # ---------------------------------------------------------
    gold_df = spark.sql("""
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            COUNT(o.order_id)        AS total_orders,
            ROUND(SUM(o.total_amount), 2)  AS total_revenue,
            ROUND(AVG(o.total_amount), 2)  AS avg_order_value
        FROM silver_orders o
        JOIN silver_products p
            ON o.product_id = p.product_id
        GROUP BY
            p.product_id,
            p.product_name,
            p.category
        ORDER BY total_revenue DESC
    """)

    # ---------------------------------------------------------
    # 6. Write to Gold Delta Table
    # ---------------------------------------------------------
    print(f"Writing gold_sales_by_product → {gold_path}")
    gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_path)

    # ---------------------------------------------------------
    # 7. Validation and Reporting
    # ---------------------------------------------------------
    result_df = spark.table(gold_path)

    print(f"\nTotal products with sales: {result_df.count()}")

    print("\nTop 5 products by revenue:")
    result_df.show(5, truncate=False)

    # Register gold table as temp view for the category breakdown query
    result_df.createOrReplaceTempView("gold_sales_by_product")

    print("Revenue by category:")
    spark.sql("""
        SELECT
            category,
            COUNT(product_id)         AS product_count,
            ROUND(SUM(total_revenue), 2) AS category_revenue
        FROM gold_sales_by_product
        GROUP BY category
        ORDER BY category_revenue DESC
    """).show(truncate=False)

    print("gold_sales_by_product written successfully.")

if __name__ == "__main__":
    main()
