from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from pyspark.sql.functions import current_timestamp, lit, col, min as spark_min, max as spark_max

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    # Databricks provides a 'spark' session by default.
    # Included here so the script is self-contained and can run
    # both in a Databricks notebook and as a standalone script.
    spark = SparkSession.builder.appName("Ingest Bronze Products").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # DBFS path where the raw CSV is located
    source_path = "data/products.csv"
    # DBFS path where the Bronze Delta table will be saved
    bronze_path = "output/delta/bronze/bronze_products"

    # ---------------------------------------------------------
    # 3. Define Explicit Schema
    # ---------------------------------------------------------
    # HARD RULE: We enforce an explicit schema rather than inferring it.
    # products.csv has no intentional quality issues — all rows are clean
    # and complete, but we still enforce types for pipeline consistency.
    products_schema = StructType([
        StructField("product_id",      IntegerType(),      True),
        StructField("product_name",    StringType(),       True),
        StructField("category",        StringType(),       True),
        StructField("price",           DecimalType(10, 2), True),
        StructField("cost",            DecimalType(10, 2), True),
        StructField("stock_quantity",  IntegerType(),      True),
        StructField("reorder_level",   IntegerType(),      True)
    ])

    print(f"Reading raw data from: {source_path}")

    # ---------------------------------------------------------
    # 4. Read Raw Data
    # ---------------------------------------------------------
    # Read the CSV applying the explicit schema defined above.
    # header=True skips the first row which contains column names.
    raw_df = spark.read.csv(
        source_path,
        schema=products_schema,
        header=True
    )

    # ---------------------------------------------------------
    # 5. Add Metadata Columns
    # ---------------------------------------------------------
    # HARD RULE: No cleaning, filtering, or transformations here.
    # We ONLY append the two required audit/lineage metadata columns.
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file_name", lit("products.csv"))

    print(f"Writing to Bronze Delta table at: {bronze_path}")

    # ---------------------------------------------------------
    # 6. Write to Bronze Delta Table
    # ---------------------------------------------------------
    # mode("overwrite") fully replaces the table on each run,
    # appropriate here as products.csv is a full daily snapshot.
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

    # --- Core Validation Metrics ---
    total_rows = validate_df.count()

    # Sum all NULL values across all columns to confirm data completeness
    null_counts = {c: validate_df.filter(col(c).isNull()).count() for c in validate_df.columns}
    total_nulls = sum(null_counts.values())

    # Count how many rows have cost correctly less than price (data integrity check)
    cost_lt_price = validate_df.filter(col("cost") < col("price")).count()

    # Fetch a sample timestamp to display in the summary
    sample_timestamp = validate_df.select("ingestion_timestamp").first()[0] if total_rows > 0 else "N/A"

    # Print ingestion summary
    print("--- Ingestion Summary ---")
    print(f"Source file:               products.csv")
    print(f"Rows Ingested:             {total_rows}  (expected: 500)")
    print(f"Ingestion Timestamp:       {sample_timestamp}")
    print(f"Total NULL values:         {total_nulls}  (expected: 0)")
    print(f"Rows where cost < price:   {cost_lt_price}  (expected: 500)")

    # Print category distribution for a quick sanity check on categorical spread
    print("\n--- Category Distribution ---")
    validate_df.groupBy("category").count().orderBy("count", ascending=False).show(truncate=False)

    # --- Data Profiling Section ---
    # This extra section provides a quick numeric range sanity check
    # to confirm that the generated data falls within expected bounds.
    print("--- Numeric Range Profiling ---")
    profiling = validate_df.agg(
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        spark_min("cost").alias("min_cost"),
        spark_max("cost").alias("max_cost"),
        spark_min("stock_quantity").alias("min_stock"),
        spark_max("stock_quantity").alias("max_stock")
    ).collect()[0]

    print(f"Price Range:          {profiling['min_price']} — {profiling['max_price']}  (expected: 5.00 — 500.00)")
    print(f"Cost Range:           {profiling['min_cost']} — {profiling['max_cost']}")
    print(f"Stock Qty Range:      {profiling['min_stock']} — {profiling['max_stock']}  (expected: 0 — 1000)")

    print("\nFirst 5 rows of Bronze Table:")
    validate_df.show(5, truncate=False)

if __name__ == "__main__":
    main()
