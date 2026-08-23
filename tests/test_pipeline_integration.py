# Intended to be run in a Databricks notebook context where `spark` is available.
# This script reads directly from the Delta tables on DBFS using PySpark.

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    
    spark = SparkSession.builder.getOrCreate()
except ImportError:
    spark = None
    print("PySpark not available. This script is meant to be run in Databricks.")

def test_bronze_row_counts():
    print("\n--- Test 1: bronze_row_counts ---")
    if not spark: return False
    
    try:
        c_df = spark.read.format("delta").load("/FileStore/delta/bronze/bronze_customers")
        o_df = spark.read.format("delta").load("/FileStore/delta/bronze/bronze_orders")
        p_df = spark.read.format("delta").load("/FileStore/delta/bronze/bronze_products")
        
        c_count = c_df.count()
        o_count = o_df.count()
        p_count = p_df.count()
        
        passed = (c_count == 10010) and (o_count == 100020) and (p_count == 500)
        
        if passed:
            print(f"PASSED (Customers: {c_count}, Orders: {o_count}, Products: {p_count})")
        else:
            print(f"FAILED: Expected 10010, 100020, 500. Got {c_count}, {o_count}, {p_count}")
        return passed
    except Exception as e:
        print(f"FAILED with error: {str(e)}")
        return False

def test_bronze_metadata_columns_exist():
    print("\n--- Test 2: bronze_metadata_columns_exist ---")
    if not spark: return False
    
    passed = True
    tables = ['bronze_customers', 'bronze_orders', 'bronze_products']
    for table in tables:
        try:
            df = spark.read.format("delta").load(f"/FileStore/delta/bronze/{table}")
            cols = df.columns
            if 'ingestion_timestamp' not in cols or 'source_file_name' not in cols:
                print(f"FAILED: Missing metadata columns in {table}")
                passed = False
                continue
                
            null_ts = df.filter(col('ingestion_timestamp').isNull()).count()
            null_fn = df.filter(col('source_file_name').isNull()).count()
            
            if null_ts > 0 or null_fn > 0:
                print(f"FAILED: NULLs found in metadata columns for {table}")
                passed = False
        except Exception as e:
            print(f"FAILED reading {table} with error: {str(e)}")
            passed = False
            
    if passed:
        print("PASSED")
    return passed

def test_silver_quality_flags_match_planted_issues():
    print("\n--- Test 3: silver_quality_flags_match_planted_issues ---")
    if not spark: return False
    
    try:
        c_df = spark.read.format("delta").load("/FileStore/delta/silver/silver_customers")
        o_df = spark.read.format("delta").load("/FileStore/delta/silver/silver_orders")
        p_df = spark.read.format("delta").load("/FileStore/delta/silver/silver_products")
        
        c_null_email = c_df.filter(col("quality_check_result").contains("NULL email")).count()
        c_dup = c_df.filter(col("quality_check_result").contains("DUPLICATE")).count()
        
        o_null_c = o_df.filter(col("quality_check_result").contains("NULL customer_id")).count()
        o_null_p = o_df.filter(col("quality_check_result").contains("NULL product_id")).count()
        o_orph_c = o_df.filter(col("quality_check_result").contains("ORPHAN customer_id")).count()
        o_orph_p = o_df.filter(col("quality_check_result").contains("ORPHAN product_id")).count()
        o_dup_o = o_df.filter(col("quality_check_result").contains("DUPLICATE order_id")).count()
        
        p_fail = p_df.filter(col("quality_check_result") != "PASS").count()
        
        expected = {
            "silver_customers 'NULL email'": (c_null_email, 50),
            "silver_customers 'DUPLICATE'": (c_dup, 10),
            "silver_orders 'NULL customer_id'": (o_null_c, 100),
            "silver_orders 'NULL product_id'": (o_null_p, 200),
            "silver_orders 'ORPHAN customer_id'": (o_orph_c, 50),
            "silver_orders 'ORPHAN product_id'": (o_orph_p, 30),
            "silver_orders 'DUPLICATE order_id'": (o_dup_o, 20),
            "silver_products failed rows": (p_fail, 0)
        }
        
        passed = True
        for desc, (actual, exp) in expected.items():
            if actual != exp:
                print(f"FAILED: {desc}. Expected {exp}, got {actual}")
                passed = False
                
        if passed:
            print("PASSED")
        return passed
    except Exception as e:
        print(f"FAILED with error: {str(e)}")
        return False

def test_gold_tables_exist_and_have_data():
    print("\n--- Test 4: gold_tables_exist_and_have_data ---")
    if not spark: return False
    
    tables = [
        "gold_sales_by_product", 
        "gold_revenue_by_customer",
        "gold_daily_trends", 
        "gold_weekly_trends",
        "gold_customer_segmentation", 
        "gold_customer_segment_detail"
    ]
    
    passed = True
    for table in tables:
        try:
            df = spark.read.format("delta").load(f"/FileStore/delta/gold/{table}")
            c = df.count()
            if c == 0:
                print(f"FAILED: {table} is empty")
                passed = False
        except Exception as e:
            print(f"FAILED: Error reading {table}: {str(e)}")
            passed = False
            
    if passed:
        print("PASSED")
    return passed

def test_gold_segmentation_has_exactly_four_segments():
    print("\n--- Test 5: gold_segmentation_has_exactly_four_segments ---")
    if not spark: return False
    
    try:
        df = spark.read.format("delta").load("/FileStore/delta/gold/gold_customer_segmentation")
        segments = [row["segment_type"] for row in df.select("segment_type").distinct().collect()]
        
        expected_segments = {"High-Value", "Repeat", "One-Time", "Inactive"}
        actual_segments = set(segments)
        
        has_null = df.filter(col("segment_type").isNull()).count() > 0
        
        passed = True
        if actual_segments != expected_segments:
            print(f"FAILED: Expected segments {expected_segments}, got {actual_segments}")
            passed = False
        
        if has_null:
            print("FAILED: NULL segment_type found")
            passed = False
            
        if passed:
            print("PASSED")
        return passed
    except Exception as e:
        print(f"FAILED with error: {str(e)}")
        return False

def test_gold_uses_pass_rows_only():
    print("\n--- Test 6: gold_uses_pass_rows_only ---")
    if not spark: return False
    
    try:
        gold_df = spark.read.format("delta").load("/FileStore/delta/gold/gold_revenue_by_customer")
        silver_df = spark.read.format("delta").load("/FileStore/delta/silver/silver_customers")
        
        gold_count = gold_df.count()
        silver_pass_count = silver_df.filter(col("quality_check_result") == "PASS").count()
        
        if gold_count <= silver_pass_count:
            print(f"PASSED (Gold rows: {gold_count} <= Silver PASS rows: {silver_pass_count})")
            return True
        else:
            print(f"FAILED: Gold rows ({gold_count}) exceeds Silver PASS rows ({silver_pass_count})")
            return False
    except Exception as e:
        print(f"FAILED with error: {str(e)}")
        return False

def test_gold_revenue_values_are_positive():
    print("\n--- Test 7: gold_revenue_values_are_positive ---")
    if not spark: return False
    
    try:
        sales_df = spark.read.format("delta").load("/FileStore/delta/gold/gold_sales_by_product")
        rev_df = spark.read.format("delta").load("/FileStore/delta/gold/gold_revenue_by_customer")
        
        neg_sales = sales_df.filter(col("total_revenue") <= 0).count()
        neg_rev = rev_df.filter(col("avg_order_value") <= 0).count()
        
        passed = True
        if neg_sales > 0:
            print(f"FAILED: {neg_sales} products have total_revenue <= 0")
            passed = False
        if neg_rev > 0:
            print(f"FAILED: {neg_rev} customers have avg_order_value <= 0")
            passed = False
            
        if passed:
            print("PASSED")
        return passed
    except Exception as e:
        print(f"FAILED with error: {str(e)}")
        return False

def run_all_tests():
    print("Starting Integration Tests...\n")
    tests = [
        test_bronze_row_counts,
        test_bronze_metadata_columns_exist,
        test_silver_quality_flags_match_planted_issues,
        test_gold_tables_exist_and_have_data,
        test_gold_segmentation_has_exactly_four_segments,
        test_gold_uses_pass_rows_only,
        test_gold_revenue_values_are_positive
    ]
    
    passed_count = 0
    for t in tests:
        if t():
            passed_count += 1
            
    print(f"\nFinal Summary: {passed_count}/{len(tests)} tests passed.")

if __name__ == "__main__":
    run_all_tests()
