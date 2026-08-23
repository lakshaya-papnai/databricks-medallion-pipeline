from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DecimalType
from pyspark.sql.functions import current_timestamp, lit, col

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session
    # ---------------------------------------------------------
    # Databricks already provides a 'spark' session object by default.
    # We include this initialization so the script is self-contained 
    # and can run both in a Databricks notebook and as a standalone script.
    spark = SparkSession.builder.appName("Ingest Bronze Customers").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()

    # ---------------------------------------------------------
    # 2. Define Paths
    # ---------------------------------------------------------
    # DBFS path where the raw CSV is located
    source_path = "data/customers.csv"
    # DBFS path where the Bronze Delta table will be saved
    bronze_path = "output/delta/bronze/bronze_customers"

    # ---------------------------------------------------------
    # 3. Define Explicit Schema
    # ---------------------------------------------------------
    # HARD RULE: We enforce an explicit schema rather than inferring it,
    # ensuring data types are strictly controlled from the very first read.
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_value", DecimalType(10, 2), True)
    ])

    print(f"Reading raw data from: {source_path}")
    
    # ---------------------------------------------------------
    # 4. Read Raw Data
    # ---------------------------------------------------------
    raw_df = spark.read.csv(
        source_path,
        schema=customer_schema,
        header=True
    )

    # ---------------------------------------------------------
    # 5. Add Metadata Columns
    # ---------------------------------------------------------
    # HARD RULE: No cleaning, filtering, or transformations here.
    # We ONLY add the required metadata columns to track lineage and ingestion time.
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file_name", lit("customers.csv"))

    print(f"Writing to Bronze Delta table at: {bronze_path}")
    
    # ---------------------------------------------------------
    # 6. Write to Bronze Delta Table
    # ---------------------------------------------------------
    # We use mode "overwrite" to completely replace the table if it already exists,
    # which is standard for an initial load or a daily full snapshot.
    bronze_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_path)

    # ---------------------------------------------------------
    # 7. Validation and Summary Reporting
    # ---------------------------------------------------------
    print("Validating Bronze table...\n")
    
    # Read the data back from the Delta table to confirm it was written correctly
    validate_df = spark.read.format("delta").load(bronze_path)
    
    # Calculate required metrics
    total_rows = validate_df.count()
    null_emails = validate_df.filter(col("email").isNull()).count()
    
    # Count how many unique customer_ids appear more than once
    duplicate_customers = validate_df.groupBy("customer_id").count().filter(col("count") > 1).count()
    
    # Fetch a sample timestamp to print in the summary
    sample_timestamp = validate_df.select("ingestion_timestamp").first()[0] if total_rows > 0 else "N/A"

    # Print summary report
    print("--- Ingestion Summary ---")
    print(f"Source file: customers.csv")
    print(f"Rows Ingested: {total_rows}")
    print(f"Ingestion Timestamp: {sample_timestamp}")
    print(f"NULL emails preserved: {null_emails}")
    print(f"Duplicate customer_ids preserved: {duplicate_customers}")
    
    print("\nFirst 5 rows of Bronze Table:")
    validate_df.show(5, truncate=False)

if __name__ == "__main__":
    main()
