from pyspark.sql import SparkSession

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Gold - Customer Segmentation") \
        \
        .getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # Gold reads from final Silver tables ONLY — never from Bronze or raw CSVs
    silver_orders_path    = "workspace.default.silver_orders"
    silver_customers_path = "workspace.default.silver_customers"

    gold_segmentation_path = "workspace.default.gold_customer_segmentation"
    gold_detail_path       = "workspace.default.gold_customer_segment_detail"

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
    # 5. Step 1 — Compute Per-Customer Metrics
    # ---------------------------------------------------------
    # For each customer calculate:
    #   - total_completed_orders: count of Completed orders only
    #   - total_revenue: sum of total_amount for Completed orders only
    # LEFT JOIN so customers with zero completed orders are included (Inactive segment).
    customer_metrics_df = spark.sql("""
        SELECT
            c.customer_id,
            c.customer_name,
            c.customer_segment,
            COUNT(CASE WHEN o.order_status = 'Completed' THEN 1 END)          AS total_completed_orders,
            ROUND(COALESCE(SUM(CASE WHEN o.order_status = 'Completed'
                                    THEN o.total_amount END), 0), 2)           AS total_revenue
        FROM silver_customers c
        LEFT JOIN silver_orders o
            ON c.customer_id = o.customer_id
        GROUP BY
            c.customer_id,
            c.customer_name,
            c.customer_segment
    """)

    customer_metrics_df.createOrReplaceTempView("customer_metrics")

    # ---------------------------------------------------------
    # 6. Step 2 — Calculate Revenue Percentile Rank
    # ---------------------------------------------------------
    # PERCENT_RANK() assigns a value from 0.0 to 1.0.
    # rank = 0.0 → highest revenue customer (we order DESC so rank 0 = top earner).
    # This is used to define the "High-Value" top-20% segment.
    ranked_df = spark.sql("""
        SELECT
            *,
            PERCENT_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_percent_rank
        FROM customer_metrics
    """)

    ranked_df.createOrReplaceTempView("customer_ranked")

    # ---------------------------------------------------------
    # 7. Step 3 — Assign Segment using Priority CASE WHEN
    # ---------------------------------------------------------
    # Priority order (first matching rule wins):
    #   1. High-Value  — top 20% by revenue (percent_rank <= 0.20)
    #   2. Inactive    — 0 completed orders
    #   3. Repeat      — 2+ completed orders
    #   4. One-Time    — exactly 1 completed order
    customer_segmented_df = spark.sql("""
        SELECT
            customer_id,
            customer_name,
            customer_segment,
            total_completed_orders,
            total_revenue,
            revenue_percent_rank,
            CASE
                WHEN revenue_percent_rank <= 0.20     THEN 'High-Value'
                WHEN total_completed_orders = 0        THEN 'Inactive'
                WHEN total_completed_orders >= 2       THEN 'Repeat'
                WHEN total_completed_orders = 1        THEN 'One-Time'
                ELSE 'Unknown'
            END AS segment_type
        FROM customer_ranked
    """)

    # ---------------------------------------------------------
    # 8. Write Customer-Level Detail Table
    # ---------------------------------------------------------
    # This detail table is useful for dashboard drill-downs and
    # allows downstream queries to filter or explore individual customers.
    print(f"Writing gold_customer_segment_detail → {gold_detail_path}")
    customer_segmented_df.write.format("delta").mode("overwrite").saveAsTable(gold_detail_path)

    # Re-read from Delta for correctness
    detail_df = spark.table(gold_detail_path)
    detail_df.createOrReplaceTempView("customer_segmented")

    # ---------------------------------------------------------
    # 9. Step 4 — Aggregate by Segment Type
    # ---------------------------------------------------------
    segmentation_summary_df = spark.sql("""
        SELECT
            segment_type,
            COUNT(customer_id)              AS customer_count,
            ROUND(AVG(total_revenue), 2)    AS avg_revenue,
            ROUND(SUM(total_revenue), 2)    AS total_revenue
        FROM customer_segmented
        GROUP BY segment_type
        ORDER BY total_revenue DESC
    """)

    # ---------------------------------------------------------
    # 10. Write Segment Summary Table to Gold
    # ---------------------------------------------------------
    print(f"Writing gold_customer_segmentation → {gold_segmentation_path}")
    segmentation_summary_df.write.format("delta").mode("overwrite").saveAsTable(gold_segmentation_path)

    # ---------------------------------------------------------
    # 11. Validation and Reporting
    # ---------------------------------------------------------
    summary_df = spark.table(gold_segmentation_path)
    detail_result = spark.table(gold_detail_path)

    total_customers = detail_result.count()
    print(f"\nTotal customers segmented: {total_customers}")

    # Verify no customer has a NULL segment_type (every customer must fall into exactly one bucket)
    null_segments = detail_result.filter("segment_type IS NULL OR segment_type = 'Unknown'").count()
    print(f"Customers with NULL/Unknown segment: {null_segments}  (expected: 0)")

    print("\nSegment summary:")
    summary_df.show(truncate=False)

    print("\nCount per segment_type (from detail table for cross-check):")
    detail_result.groupBy("segment_type").count().orderBy("count", ascending=False).show(truncate=False)

    print("gold_customer_segmentation and gold_customer_segment_detail written successfully.")

if __name__ == "__main__":
    main()
