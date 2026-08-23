#!/bin/bash

# Exit on any command failure during setup
set -e

echo "Setting up local Spark environment..."

if [ ! -d "venv_spark" ]; then
    echo "Creating virtual environment venv_spark..."
    python3 -m venv venv_spark
fi

echo "Activating venv_spark..."
source venv_spark/bin/activate

echo "Installing required packages..."
pip install -q pyspark==3.4.0 delta-spark==2.4.0 pandas faker numpy pytest

echo "Creating output directories..."
mkdir -p output/delta/bronze/
mkdir -p output/delta/silver/
mkdir -p output/delta/gold/

# Force the system to use the pip-installed PySpark rather than any globally installed Spark
export SPARK_HOME=$(python -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))")
export PATH=$SPARK_HOME/bin:$PATH
unset SPARK_LOCAL_DIRS

# Set Delta Lake extensions environment variable
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:2.4.0 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

# Disable exit on error for the pipeline run so we can handle custom error messages
set +e

BRONZE_STATUS="FAILED"
SILVER_STATUS="FAILED"
GOLD_STATUS="FAILED"

echo ""
echo "--- RUNNING BRONZE LAYER ---"
python src/bronze/ingest_all.py
if [ $? -eq 0 ]; then
    BRONZE_STATUS="PASSED"
else
    echo "Error: Bronze layer failed. Aborting pipeline."
    echo ""
    echo "BRONZE: FAILED"
    echo "SILVER: SKIPPED"
    echo "GOLD: SKIPPED"
    exit 1
fi

echo ""
echo "--- RUNNING SILVER LAYER ---"
python src/silver/01_quality_completeness.py && \
python src/silver/02_quality_uniqueness.py && \
python src/silver/03_quality_type_validation.py && \
python src/silver/04_quality_referential_integrity.py && \
python src/silver/05_quality_business_logic.py && \
python src/silver/create_silver_tables.py
if [ $? -eq 0 ]; then
    SILVER_STATUS="PASSED"
else
    echo "Error: Silver layer failed. Aborting pipeline."
    echo ""
    echo "BRONZE: PASSED"
    echo "SILVER: FAILED"
    echo "GOLD: SKIPPED"
    exit 1
fi

echo ""
echo "--- RUNNING GOLD LAYER ---"
python src/gold/create_gold_tables.py
if [ $? -eq 0 ]; then
    GOLD_STATUS="PASSED"
else
    echo "Error: Gold layer failed. Aborting pipeline."
    echo ""
    echo "BRONZE: PASSED"
    echo "SILVER: PASSED"
    echo "GOLD: FAILED"
    exit 1
fi

echo ""
echo "--- RUNNING LOCAL TESTS ---"
pytest tests/test_data_quality.py -v > pytest_output.log 2>&1
TEST_EXIT_CODE=$?
cat pytest_output.log

# Extract exact passed/failed numbers using Python
TESTS_STATS=$(python3 -c "
import sys, re
out = sys.stdin.read()
lines = out.strip().split('\n')
for line in reversed(lines):
    if ('passed' in line or 'failed' in line) and ('===' in line):
        p = re.search(r'(\d+) passed', line)
        f = re.search(r'(\d+) failed', line)
        passed = p.group(1) if p else '0'
        failed = f.group(1) if f else '0'
        print(f'{passed} passed, {failed} failed')
        sys.exit(0)
print('0 passed, 0 failed')
" < pytest_output.log)

echo ""
echo "======================================"
echo "          PIPELINE SUMMARY            "
echo "======================================"
echo "BRONZE: $BRONZE_STATUS"
echo "SILVER: $SILVER_STATUS"
echo "GOLD: $GOLD_STATUS"
echo "TESTS: $TESTS_STATS"

if [ $TEST_EXIT_CODE -ne 0 ]; then
    exit 1
fi
exit 0
