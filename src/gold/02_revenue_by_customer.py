from pyspark.sql import SparkSession

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Gold - Revenue by Customer") \
        \
        .getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # Gold reads from final Silver tables ONLY — never from Bronze or raw CSVs
    silver_orders_path    = "workspace.default.silver_orders"
    silver_customers_path = "workspace.default.silver_customers"
    gold_path             = "workspace.default.gold_revenue_by_customer"

    # ---------------------------------------------------------
    # 3. Load Silver Tables — PASS rows only
    # ---------------------------------------------------------
    # HARD RULE: Only rows that passed all quality checks feed Gold aggregations.
    orders_df    = spark.table(silver_orders_path) \
                        .filter("quality_check_result = 'PASS'")

    customers_df = spark.table(silver_customers_path) \
                        .filter("quality_check_result = 'PASS'")

    # ---------------------------------------------------------
    # 4. Register Temp Views for spark.sql()
    # ---------------------------------------------------------
    orders_df.createOrReplaceTempView("silver_orders")
    customers_df.createOrReplaceTempView("silver_customers")

    # ---------------------------------------------------------
    # 5. Run Aggregation via spark.sql()
    # ---------------------------------------------------------
    # lifetime_value is a customer-level attribute carried through as-is.
    # We use MAX() to de-aggregate it safely within the GROUP BY — it is
    # the same value for every row of the same customer after the join.
    gold_df = spark.sql("""
        SELECT
            c.customer_id,
            c.customer_name,
            c.customer_segment,
            COUNT(o.order_id)               AS total_orders,
            ROUND(SUM(o.total_amount), 2)   AS total_revenue,
            ROUND(AVG(o.total_amount), 2)   AS avg_order_value,
            MAX(c.lifetime_value)           AS lifetime_value_actual
        FROM silver_orders o
        JOIN silver_customers c
            ON o.customer_id = c.customer_id
        GROUP BY
            c.customer_id,
            c.customer_name,
            c.customer_segment
        ORDER BY total_revenue DESC
    """)

    # ---------------------------------------------------------
    # 6. Write to Gold Delta Table
    # ---------------------------------------------------------
    print(f"Writing gold_revenue_by_customer → {gold_path}")
    gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_path)

    # ---------------------------------------------------------
    # 7. Validation and Reporting
    # ---------------------------------------------------------
    result_df = spark.table(gold_path)

    print(f"\nTotal customers with orders: {result_df.count()}")

    print("\nTop 5 customers by revenue:")
    result_df.show(5, truncate=False)

    # Register as temp view for the segment breakdown query
    result_df.createOrReplaceTempView("gold_revenue_by_customer")

    print("Revenue breakdown by customer_segment:")
    spark.sql("""
        SELECT
            customer_segment,
            COUNT(customer_id)              AS customer_count,
            ROUND(SUM(total_revenue), 2)    AS segment_revenue,
            ROUND(AVG(total_revenue), 2)    AS avg_revenue_per_customer
        FROM gold_revenue_by_customer
        GROUP BY customer_segment
        ORDER BY segment_revenue DESC
    """).show(truncate=False)

    print("gold_revenue_by_customer written successfully.")

if __name__ == "__main__":
    main()
