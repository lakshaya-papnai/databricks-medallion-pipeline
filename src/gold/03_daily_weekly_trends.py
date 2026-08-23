from pyspark.sql import SparkSession

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("Gold - Daily and Weekly Trends") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # Gold reads from final Silver tables ONLY — never from Bronze or raw CSVs
    silver_orders_path  = "output/delta/silver/silver_orders"
    gold_daily_path     = "output/delta/gold/gold_daily_trends"
    gold_weekly_path    = "output/delta/gold/gold_weekly_trends"

    # ---------------------------------------------------------
    # 3. Load Silver Orders — PASS rows and Completed orders only
    # ---------------------------------------------------------
    # HARD RULE: Only PASS rows feed Gold aggregations.
    # For trend analysis we further restrict to Completed orders only,
    # because only Completed orders represent confirmed, realized revenue.
    orders_df = spark.read.format("delta").load(silver_orders_path) \
                    .filter("quality_check_result = 'PASS'") \
                    .filter("order_status = 'Completed'")

    # ---------------------------------------------------------
    # 4. Register Temp View for spark.sql()
    # ---------------------------------------------------------
    orders_df.createOrReplaceTempView("silver_orders_completed")

    # ---------------------------------------------------------
    # 5a. Daily Trends Aggregation
    # ---------------------------------------------------------
    # Group by order_date to get a one-row-per-day revenue summary.
    daily_df = spark.sql("""
        SELECT
            order_date,
            COUNT(order_id)              AS daily_orders,
            ROUND(SUM(total_amount), 2)  AS daily_revenue,
            ROUND(AVG(total_amount), 2)  AS daily_avg_order_value
        FROM silver_orders_completed
        GROUP BY order_date
        ORDER BY order_date ASC
    """)

    # ---------------------------------------------------------
    # 5b. Weekly Trends Aggregation
    # ---------------------------------------------------------
    # Group by year + ISO week number to produce a one-row-per-week summary.
    # YEAR() and WEEKOFYEAR() are native Spark SQL functions.
    weekly_df = spark.sql("""
        SELECT
            YEAR(order_date)             AS order_year,
            WEEKOFYEAR(order_date)       AS week_number,
            COUNT(order_id)              AS weekly_orders,
            ROUND(SUM(total_amount), 2)  AS weekly_revenue,
            ROUND(AVG(total_amount), 2)  AS weekly_avg_order_value
        FROM silver_orders_completed
        GROUP BY
            YEAR(order_date),
            WEEKOFYEAR(order_date)
        ORDER BY order_year ASC, week_number ASC
    """)

    # ---------------------------------------------------------
    # 6. Write Both Tables to Gold Delta
    # ---------------------------------------------------------
    print(f"Writing gold_daily_trends  → {gold_daily_path}")
    daily_df.write.format("delta").mode("overwrite").save(gold_daily_path)

    print(f"Writing gold_weekly_trends → {gold_weekly_path}")
    weekly_df.write.format("delta").mode("overwrite").save(gold_weekly_path)

    # ---------------------------------------------------------
    # 7. Validation and Reporting
    # ---------------------------------------------------------
    daily_result  = spark.read.format("delta").load(gold_daily_path)
    weekly_result = spark.read.format("delta").load(gold_weekly_path)

    # Register for SQL validation queries
    daily_result.createOrReplaceTempView("gold_daily_trends")

    # Date range and coverage
    date_range = spark.sql("""
        SELECT
            MIN(order_date) AS earliest_order,
            MAX(order_date) AS latest_order,
            COUNT(*)        AS total_days_with_orders
        FROM gold_daily_trends
    """)
    print("\nDate range of completed orders:")
    date_range.show(truncate=False)

    print(f"Total day-level rows: {daily_result.count()}")
    print(f"Total week-level rows: {weekly_result.count()}")

    print("\nTop 5 highest revenue days:")
    spark.sql("""
        SELECT order_date, daily_orders, daily_revenue
        FROM gold_daily_trends
        ORDER BY daily_revenue DESC
        LIMIT 5
    """).show(truncate=False)

    print("\nSample of gold_weekly_trends (first 5 rows):")
    weekly_result.show(5, truncate=False)

    print("gold_daily_trends and gold_weekly_trends written successfully.")

if __name__ == "__main__":
    main()
