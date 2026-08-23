from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from pyspark.sql.functions import current_timestamp, lit, col

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    # Databricks provides a 'spark' session by default.
    # Included here so the script is self-contained and can run
    # both in a Databricks notebook and as a standalone script.
    spark = SparkSession.builder.appName("Ingest Bronze Orders").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # DBFS path where the raw CSV is located
    source_path = "data/orders.csv"
    # DBFS path where the Bronze Delta table will be saved
    bronze_path = "output/delta/bronze/bronze_orders"

    # ---------------------------------------------------------
    # 3. Define Explicit Schema
    # ---------------------------------------------------------
    # HARD RULE: We enforce an explicit schema rather than inferring it.
    # payment_date is nullable=True because it is intentionally NULL
    # for all Pending and Cancelled orders — this must be preserved as-is.
    orders_schema = StructType([
        StructField("order_id",      IntegerType(),      True),
        StructField("customer_id",   IntegerType(),      True),
        StructField("order_date",    DateType(),         True),
        StructField("product_id",    IntegerType(),      True),
        StructField("quantity",      IntegerType(),      True),
        StructField("unit_price",    DecimalType(10, 2), True),
        StructField("total_amount",  DecimalType(10, 2), True),
        StructField("order_status",  StringType(),       True),
        StructField("payment_date",  DateType(),         True)   # Nullable by design
    ])

    print(f"Reading raw data from: {source_path}")

    # ---------------------------------------------------------
    # 4. Read Raw Data
    # ---------------------------------------------------------
    # Read the CSV applying the explicit schema defined above.
    # Spark will honour NULLs in customer_id, product_id, and payment_date
    # exactly as they appear in the source file.
    raw_df = spark.read.csv(
        source_path,
        schema=orders_schema,
        header=True
    )

    # ---------------------------------------------------------
    # 5. Add Metadata Columns
    # ---------------------------------------------------------
    # HARD RULE: No cleaning, filtering, or transformations here.
    # We ONLY append the two required audit/lineage metadata columns.
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file_name", lit("orders.csv"))

    print(f"Writing to Bronze Delta table at: {bronze_path}")

    # ---------------------------------------------------------
    # 6. Write to Bronze Delta Table
    # ---------------------------------------------------------
    # mode("overwrite") fully replaces the table if it already exists,
    # which is appropriate for a daily full-load pattern.
    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_path)

    # ---------------------------------------------------------
    # 7. Validation and Summary Reporting
    # ---------------------------------------------------------
    print("Validating Bronze table...\n")

    # Read back from Delta to confirm the write was successful
    validate_df = spark.read.format("delta").load(bronze_path)

    # Calculate validation metrics
    total_rows       = validate_df.count()
    null_customers   = validate_df.filter(col("customer_id").isNull()).count()
    null_products    = validate_df.filter(col("product_id").isNull()).count()

    # Count order_ids that appear more than once (duplicate rows)
    duplicate_orders = validate_df.groupBy("order_id").count().filter(col("count") > 1).count()

    # Fetch a sample timestamp to display in the summary
    sample_timestamp = validate_df.select("ingestion_timestamp").first()[0] if total_rows > 0 else "N/A"

    # Print ingestion summary
    print("--- Ingestion Summary ---")
    print(f"Source file:               orders.csv")
    print(f"Rows Ingested:             {total_rows}  (expected: 100,020)")
    print(f"Ingestion Timestamp:       {sample_timestamp}")
    print(f"NULL customer_ids:         {null_customers}  (expected: 100)")
    print(f"NULL product_ids:          {null_products}  (expected: 200)")
    print(f"Duplicate order_ids:       {duplicate_orders}  (expected: 20)")

    print("\nFirst 5 rows of Bronze Table:")
    validate_df.show(5, truncate=False)

if __name__ == "__main__":
    main()
