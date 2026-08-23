# AI Prompt Diary — Data Generation

This file records all AI-assisted interactions related to designing and
generating the synthetic sample data used throughout this pipeline.

---

## Prompt 1: Generate generate_sample_data.py

**PROMPT SENT:**
> [.cursorrules active]
>
> Generate src/data_generation/generate_sample_data.py only.
> This runs locally, not in Databricks. It creates three CSVs
> and saves them to the data/ folder.
>
> Use faker for realistic names and emails, pandas and random for
> construction. Set random seed 42 for reproducibility.
>
> customers.csv — 10,000 rows:
> - customer_id: 1 to 10,000 sequential
> - customer_name: realistic full names via faker
> - email: realistic emails via faker
> - country: random from India, USA, UK, Germany, France, Canada, Australia
> - signup_date: 2020-01-01 to 2024-12-31
> - customer_segment: Premium/Standard/Basic with weights 20%/50%/30%
> - lifetime_value: decimal between 100.00 and 50000.00
>
> Intentional issues for customers:
> - Exactly 50 rows with NULL email
> - Exactly 10 duplicate customer_ids added at the end
>
> products.csv — 500 rows:
> - product_id sequential, product_name, category (7 categories),
>   price, cost (always less than price), stock_quantity, reorder_level
> - No intentional issues
>
> orders.csv — 100,000 rows:
> - order_id, customer_id (FK), order_date, product_id (FK),
>   quantity 1-10, unit_price from product, total_amount = qty * price,
>   order_status weighted Pending/Completed/Cancelled,
>   payment_date NULL for Pending/Cancelled, date after order_date for Completed
>
> Intentional issues for orders:
> - 100 rows: customer_id = NULL
> - 200 rows: product_id = NULL
> - 50 rows: customer_id between 10001-10050 (orphan)
> - 30 rows: product_id between 501-530 (orphan)
> - 20 rows: duplicate order_ids
>
> Critical: quality issues must affect different rows.
> No single row should have more than one issue planted.
>
> At the end print a summary: total rows per file, count of each
> issue type, confirmation files saved.
> Add comments explaining what each section does.

**AI RESPONSE SUMMARY:**
Generated generate_sample_data.py using Faker, random, pandas, and
numpy with seed 42 across all libraries. Used a non-overlapping index
pool — sampled 400 distinct indices from range(100000) and partitioned
into sub-ranges per issue type to ensure no row got two issues.
Used pandas Int64 dtype for nullable integer columns. Wrapped in
main() function. Proactively set up a venv, installed dependencies,
and ran the script to confirm output before returning it.

**WHAT I ACCEPTED:**
- Non-overlapping index approach was correct — sampling 400 distinct
  indices then partitioning ensures clean separation between issue types
- Int64 dtype for customer_id and product_id in orders is the right
  pandas choice for nullable integers with pd.NA
- main() + if __name__ == "__main__" pattern makes script importable
- Summary printout at the end clearly showed all expected counts
- Proactive venv setup saved a manual step

**WHAT I CHANGED:**
- The country list in the first version included Brazil and Japan
  which weren't in my specified list. Fixed the country list to
  exactly: India, USA, UK, Germany, France, Canada, Australia
- Product name generation used pure faker which produced unrealistic
  names like "Chair Chair Deluxe". Changed to an adjective + product
  noun combination list for more realistic e-commerce style names

**WHAT I REJECTED:**
- Cursor initially generated products with a separate description
  column that wasn't in the schema. Removed it — we follow the
  schema exactly as defined in .cursorrules, no extra columns

**FINAL DECISION:**
Accepted after fixing country list and product name generation.
Saved as src/data_generation/generate_sample_data.py.

---

## Prompt 2: Validate generated data

**PROMPT SENT:**
> Run these validation checks and return me the output:
>
> wc -l data/customers.csv data/orders.csv data/products.csv
>
> python3 -c "
> import pandas as pd
> df = pd.read_csv('data/customers.csv')
> print('--- CUSTOMERS ---')
> print('Total rows:', len(df))
> print('NULL emails:', df['email'].isna().sum())
> print('Duplicate customer_ids:', df['customer_id'].duplicated().sum())
> print('Segments:', df['customer_segment'].value_counts().to_dict())
> "
>
> python3 -c "
> import pandas as pd
> df = pd.read_csv('data/orders.csv')
> print('--- ORDERS ---')
> print('Total rows:', len(df))
> print('NULL customer_ids:', df['customer_id'].isna().sum())
> print('NULL product_ids:', df['product_id'].isna().sum())
> print('Duplicate order_ids:', df['order_id'].duplicated().sum())
> valid = df['customer_id'].dropna()
> orphan_c = valid[(valid >= 10001) & (valid <= 10050)].count()
> print('Orphan customer_ids:', orphan_c)
> valid_p = df['product_id'].dropna()
> orphan_p = valid_p[(valid_p >= 501) & (valid_p <= 530)].count()
> print('Orphan product_ids:', orphan_p)
> "
>
> python3 -c "
> import pandas as pd
> df = pd.read_csv('data/products.csv')
> print('--- PRODUCTS ---')
> print('Total rows:', len(df))
> print('Categories:', df['category'].value_counts().to_dict())
> print('Any NULLs:', df.isna().sum().sum())
> print('Cost always < Price:', (df['cost'] < df['price']).all())
> "

**AI RESPONSE SUMMARY:**
First attempt failed — system python3 didn't have pandas installed
and returned ModuleNotFoundError. Cursor switched to ./venv/bin/python
and reran. All checks passed with exact expected counts:

```
  10011 data/customers.csv
 100021 data/orders.csv
    501 data/products.csv

--- CUSTOMERS ---
Total rows: 10010
NULL emails: 50
Duplicate customer_ids: 10
Segments: {'Standard': 5107, 'Basic': 2923, 'Premium': 1980}

--- ORDERS ---
Total rows: 100020
NULL customer_ids: 100
NULL product_ids: 200
Duplicate order_ids: 20
Orphan customer_ids: 50
Orphan product_ids: 30

--- PRODUCTS ---
Total rows: 500
Categories: {'Beauty': 86, 'Books': 82, 'Sports': 81,
             'Home & Kitchen': 73, 'Electronics': 66,
             'Toys': 61, 'Clothing': 51}
Any NULLs: 0
Cost always < Price: True
```

**WHAT I ACCEPTED:**
- All counts matched expected planted issue counts exactly
- 7 categories with realistic distribution confirmed
- cost < price True for all 500 product rows
- wc -l counts are rows + 1 (header) which is correct

**WHAT I CHANGED:**
- Nothing — all outputs matched expectations

**WHAT I REJECTED:**
- Cursor suggested adding a data profiling section to the script
  itself that would auto-run validations on every execution.
  Rejected — validation should be a separate step, not baked
  into the generator. Mixing generation and validation in one
  script makes it harder to reuse the generator independently

**FINAL DECISION:**
Data files validated. All intentional quality issues confirmed.
No changes to generate_sample_data.py needed after validation.
Pipeline ready to move to Bronze ingestion.

---

## Prompt 3: DATA_GENERATION_NOTES.md

**PROMPT SENT:**
> [.cursorrules active]
>
> Write src/data_generation/DATA_GENERATION_NOTES.md only.
> This documents the design decisions behind the data generation.
>
> Cover:
> - Why faker was chosen over hardcoded values
> - Why seed 42 (reproducibility — anyone cloning the repo gets
>   identical CSVs)
> - How the non-overlapping issue index approach works and why
>   it matters (no row should have two issues — would make it
>   ambiguous which check should catch it)
> - Why each quality issue type was chosen:
>   NULL emails -> tests completeness check
>   Duplicate customer_ids -> tests uniqueness check
>   Orphan FKs -> tests referential integrity check
>   NULL FKs -> tests completeness on orders
>   Duplicate order_ids -> tests uniqueness on orders
> - Row count rationale: 10K customers, 100K orders, 500 products
>   is a realistic e-commerce ratio
> - How to regenerate the data if needed

**AI RESPONSE SUMMARY:**
Generated DATA_GENERATION_NOTES.md covering all sections with clear
explanations of each design decision. Included a regeneration section
with the exact pip install and python commands.

**WHAT I ACCEPTED:**
- The ratio explanation (roughly 10 orders per customer on average,
  200 orders per product) was a good way to justify the row counts
- The non-overlapping index explanation was clear and accurate

**WHAT I CHANGED:**
- Added a note explaining why real customer data was never used —
  all data is synthetic specifically to avoid sharing PII with
  an external AI tool. This is part of responsible AI usage and
  felt important to document explicitly

**WHAT I REJECTED:**
- Cursor added a section suggesting using Databricks to generate
  the data at scale instead of running locally. Out of scope —
  the data generator is intentionally local so it runs without
  needing a Databricks cluster

**FINAL DECISION:**
Saved as src/data_generation/DATA_GENERATION_NOTES.md with
responsible AI note added and out-of-scope Databricks suggestion
removed.