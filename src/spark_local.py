from pyspark.sql import SparkSession

def get_spark(app_name="LocalPipeline"):
    """
    Creates and returns a SparkSession configured for local execution and Delta Lake.
    Uses all local CPU cores (local[*]) and registers the Delta catalog and extensions.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    return spark
