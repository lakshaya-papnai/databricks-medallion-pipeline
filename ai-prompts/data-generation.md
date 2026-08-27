# AI Prompt Diary — Data Generation

This file records the chat history and decisions made while designing and generating the synthetic sample data for the pipeline.

---

## Prompt 1: Generate generate_sample_data.py

**PROMPT SENT:**
> [.cursorrules active]
> 
> Generate src/data_generation/generate_sample_data.py only. It should run locally, not in Databricks. It needs to create three CSV files in the data/ folder.
> use faker for names/emails, pandas and random for the rest. seed 42 so we can reproduce it.
> customers.csv: 10k rows. cols: customer_id (1-10000), customer_name, email, country (India, USA, UK, Germany, France, Canada, Australia), signup_date (2020-2024), customer_segment (Premium 20, Standard 50, Basic 30), lifetime_value (dec 100 to 50k)
> plant these issues: 50 rows with NULL email, 10 duplicate customer_ids at the end
> 
> products.csv: 500 rows. cols: product_id seq, product_name, category (pick 7 random), price, cost (always < price), stock_quantity, reorder_level. no issues planted here.
> 
> orders.csv: 100k rows. cols: order_id, customer_id (FK), order_date, product_id (FK), quantity (1-10), unit_price (from prod), total_amount (qty * price), order_status (Pending/Completed/Cancelled), payment_date (NULL if pending/cancelled, else after order_date)
> plant these issues: 100 rows customer_id = NULL, 200 rows product_id = NULL, 50 orphan customer_id (10001-10050), 30 orphan product_id (501-530), 20 duplicate order_ids
> 
> super critical: issues must be on diff rows. no row can have more than 1 issue planted.
> print summary at end: row counts, issue counts, confirm saved. add some comments too.

**AI RESPONSE:**
It wrote `generate_sample_data.py` using Faker, random, and pandas, setting seed 42 everywhere. To handle the non-overlapping issues, it sampled 400 distinct indices from `range(100000)` and partitioned them out so no row got double-hit. Used pandas Int64 for the nullable integer cols. Even wrapped it in a `main()` function and proactively set up a venv to test it before giving me the code.

**WHAT I KEPT:**
- The index sampling idea was smart. Grabbing 400 distinct ones and splitting them up made sure the issues stayed separated.
- Using Int64 for `customer_id` and `product_id` was the right call for nullable ints in pandas.
- The `main()` block and the summary printout at the end were helpful.
- Nice that it set up the venv for me.

**WHAT I TWEAKED:**
- It threw Brazil and Japan into the country list even though I didn't ask for them. Swapped it back to just India, USA, UK, Germany, France, Canada, Australia.
- The product names were terrible (faker just gave stuff like "Chair Chair Deluxe"). I rewrote it to use a quick adjective + noun list so it looked like actual e-commerce stuff.

**WHAT I THREW OUT:**
- It tried to add a description column to products that wasn't in my `.cursorrules`. Deleted it. We stick to the schema.

**VERDICT:**
Accepted it after fixing the countries and the product names. Saved it down to `src/data_generation/generate_sample_data.py`.

---

## Prompt 2: Validate generated data

**PROMPT SENT:**
> run these validation checks on the csvs and show me the output:
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

**AI RESPONSE:**
Failed at first because system python3 didn't have pandas. It figured it out, switched to `./venv/bin/python` and ran it again. All the checks passed and spit out the exact counts I wanted.

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

**WHAT I KEPT:**
- All the counts matched my planted issues perfectly. 
- Categories looked realistic and cost was always less than price for all 500 rows.
- The `wc -l` counts included the headers, which is fine.

**WHAT I TWEAKED:**
- Nothing. The data looked good as is.

**WHAT I THREW OUT:**
- It suggested writing a data profiling function right into the generator script so it would run every time. I said no. Validation should be a separate step. I don't want the generator bogged down with checks if I just need to spit out fresh data later.

**VERDICT:**
Data files are solid. Issues confirmed. Ready to move on to the Bronze layer.

---

## Prompt 3: DATA_GENERATION_NOTES.md

**PROMPT SENT:**
> [.cursorrules active]
> 
> write src/data_generation/DATA_GENERATION_NOTES.md only. need to document the design decisions for the synthetic data.
> 
> cover this stuff:
> - why faker instead of hardcoded values
> - why seed 42 (so anyone cloning the repo gets the exact same csvs)
> - explain that non-overlapping index thing. why it matters (if a row has two issues we won't know which check caught it)
> - list out why i chose these specific issues:
>   NULL emails -> completeness check
>   duplicate customer_ids -> uniqueness check
>   orphan FKs -> ref integrity
>   NULL FKs -> completeness on orders
>   duplicate order_ids -> uniqueness on orders
> - explain the row counts. 10k customers, 100k orders, 500 prods is a decent e-commerce ratio
> - add quick instructions on how to regen the data

**AI RESPONSE:**
It wrote up `DATA_GENERATION_NOTES.md` hitting all those points and explaining the design pretty well. Threw in the exact pip and python commands to regenerate it at the end.

**WHAT I KEPT:**
- The explanation of the row ratios (like 10 orders per customer) made sense.
- The breakdown of how the non-overlapping indices worked was spot on.

**WHAT I TWEAKED:**
- I added a note explaining *why* I didn't use real data — strictly to avoid sharing PII with an external AI. Felt like an important responsible AI thing to call out.

**WHAT I THREW OUT:**
- It added a section suggesting we could use Databricks to generate the data at scale later instead of doing it locally. Ripped that out, the whole point of this script is to run locally without needing a cluster.

**VERDICT:**
Saved it to `src/data_generation/DATA_GENERATION_NOTES.md`. Tossed the Databricks suggestion.