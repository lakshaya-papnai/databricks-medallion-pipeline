# Data Generation Notes

**Script:** `generate_sample_data.py`
**Output:** `data/customers.csv`, `data/orders.csv`, `data/products.csv`
**Run environment:** Local Python (not Databricks)

---

## Why Synthetic Data

Real e-commerce data was never considered for this project. Using real customer or order data through an AI development tool would be irresponsible regardless of anonymisation — there is no audit trail for how AI tools handle data passed in prompts. Synthetic data eliminates that risk entirely while allowing full control over the shape, volume, and quality characteristics of the dataset.

---

## Library Choices

**`Faker`**
Faker generates realistic-looking names, email addresses, and geographic data. Using Faker rather than hardcoded strings means the dataset looks like real production data — varied names, realistic email formats, plausible country distributions. This makes quality issues (like a NULL email) stand out naturally rather than being buried in obviously fake test data.

**`pandas`**
Used for DataFrame construction, CSV writing, and applying the intentional quality issue overlays. Pandas' `Int64` nullable integer dtype (distinct from standard `int64`) was specifically required for `customer_id` and `product_id` in the orders table to support `pd.NA` values — standard `int64` cannot hold null values in pandas.

**`numpy`**
Used for `np.random.choice()` with weighted probabilities (customer segment distribution: 20% Premium, 50% Standard, 30% Basic) and for setting the random seed consistently across all NumPy operations.

---

## Random Seed: 42

All three random number generators were seeded with `42`:

```python
random.seed(42)
np.random.seed(42)
Faker.seed(42)
```

A fixed seed guarantees that anyone who clones the repository and runs `generate_sample_data.py` will produce byte-identical CSV files. This is important for two reasons:
1. The Silver layer validation expected counts (50 NULL emails, 10 duplicates, etc.) are hard-coded. If the data changed between runs, those expected counts would no longer match.
2. Reproducibility is a basic requirement for any data engineering project — a pipeline that only works with one specific dataset is not a pipeline, it is a script.

---

## Row Count Rationale

| Table | Rows | Reasoning |
|:---|:---:|:---|
| `customers` | 10,000 | Base dimension table — realistic SME-scale customer base |
| `orders` | 100,000 | 10:1 ratio to customers — each customer places ~10 orders on average over the data period |
| `products` | 500 | Small product catalogue — realistic for a focused e-commerce store |

The 10,000 : 100,000 : 500 ratio mirrors realistic e-commerce data proportions. A 100,000-row orders table is large enough to make quality issues feel like realistic noise (0.7% of total rows) rather than obvious test pollution.

---

## Intentional Quality Issue Design

All 460 quality issues were distributed across **non-overlapping rows**. A pool of 400 order row indices was generated using `random.sample(range(100000), 400)` and then partitioned into non-overlapping subsets:

| Indices 0–99 | → NULL `customer_id` (100 rows) |
| Indices 100–299 | → NULL `product_id` (200 rows) |
| Indices 300–349 | → Orphan `customer_id` (50 rows, IDs 10001–10050) |
| Indices 350–379 | → Orphan `product_id` (30 rows, IDs 501–530) |
| Indices 380–399 | → Duplicate `order_id` (20 rows, reuse existing IDs) |

Customer quality issues (NULL emails and duplicate `customer_id`s) were handled separately and also assigned to non-overlapping customer rows.

**Why non-overlapping?** If a row had both a NULL `customer_id` and an orphan `product_id`, it would be ambiguous which Silver check should catch it. Keeping issues separate means each check is tested independently and expected counts are unambiguous.

### Why each issue type was chosen

| Issue | Purpose | Silver check it exercises |
|:---|:---|:---|
| NULL emails | Tests whether completeness check catches missing critical fields | `01_quality_completeness.py` |
| Duplicate `customer_id` | Tests whether uniqueness check uses window functions correctly | `02_quality_uniqueness.py` |
| NULL `customer_id` in orders | Tests FK completeness on the fact table | `01_quality_completeness.py` |
| NULL `product_id` in orders | Tests FK completeness on the fact table | `01_quality_completeness.py` |
| Orphan `customer_id` | Tests LEFT JOIN referential integrity logic | `04_quality_referential_integrity.py` |
| Orphan `product_id` | Tests LEFT JOIN referential integrity logic | `04_quality_referential_integrity.py` |
| Duplicate `order_id` | Tests uniqueness check on the fact table PK | `02_quality_uniqueness.py` |

Products were kept clean intentionally — the pipeline needs at least one source table that passes all checks cleanly, to confirm that the Silver layer does not flag correct data as failures.

---

## How to Regenerate the Data

If the CSV files are lost or corrupted, regenerate them with:

```bash
# From the project root
cd /path/to/C1-Assessment

# Activate the virtual environment (if using one)
source venv/bin/activate   # Linux/macOS
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install faker pandas numpy

# Run the generator
python src/data_generation/generate_sample_data.py
```

The output will be written to `data/customers.csv`, `data/orders.csv`, and `data/products.csv`. Because seed `42` is hardcoded in the script, the output will be identical to the original files every time.

After regenerating, re-upload the CSVs to DBFS before re-running the Bronze layer (see `README.md` Step 3).
