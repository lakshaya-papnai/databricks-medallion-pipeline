# Test Strategy

## Two-Tier Testing Approach

This project uses a two-tier testing strategy to validate data quality rules and pipeline output:

1. **Local Tests (`tests/test_data_quality.py`)**: Uses Pandas against the raw CSV files in `data/` to validate the core data quality check logic locally before deploying to Databricks.
2. **Integration Tests (`tests/test_pipeline_integration.py`)**: Uses PySpark inside Databricks to validate the end-to-end output across the Medallion architecture (Bronze, Silver, and Gold Delta tables).

### Why this split?
Local tests are fast and allow us to catch logic errors early without needing a running Databricks cluster. Integration tests ensure the actual Spark transformations and Delta table writes across the full pipeline execute correctly in the target environment.

## Test Oracle Approach

The intentional quality issues planted during data generation (`generate_sample_data.py`) serve as our test oracle. Because we know the exact count and type of every planted issue (e.g., exactly 50 NULL emails, 20 duplicate order IDs), our test assertions can be precise and deterministic rather than approximate.

## How to Run Tests

### Running Local Tests
Local tests can be run in your terminal:
```bash
pip install pytest pandas
pytest tests/test_data_quality.py -v
```

### Running Integration Tests
Integration tests are designed to run within the Databricks environment:
1. Copy the contents of `tests/test_pipeline_integration.py` into a new Databricks notebook.
2. Run the notebook after completing the full Bronze → Silver → Gold pipeline execution.

## What is Not Tested and Why

* **Unit Tests for Individual PySpark Transformations:** We do not test individual PySpark transformations in isolation. Refactoring the monolithic processing scripts into modular, testable functions was out of scope for this assessment timeline.
* **Performance Tests:** There are no benchmarking or performance tests. Given that the project runs on a single-node Free Edition cluster with a relatively small data scale, performance metrics would be meaningless.
