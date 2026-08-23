# Seed Data Notes

**Generator script:** `src/data_generation/generate_sample_data.py`
**Output location:** `data/customers.csv`, `data/orders.csv`, `data/products.csv`

---

## What Was Generated and Why

Three synthetic CSV files were generated to simulate the daily sales data feed of a mid-size e-commerce company:

| File | Rows | Purpose |
|:---|:---:|:---|
| `customers.csv` | 10,010 | Customer dimension — identity, segment, and lifetime value |
| `orders.csv` | 100,020 | Fact table — every transaction across the date range |
| `products.csv` | 500 | Product dimension — catalogue with pricing and stock |

The extra rows (10 in customers, 20 in orders) are the intentionally duplicated rows planted as quality issues — they are appended after the base dataset is generated.

Synthetic data was chosen over real data for two reasons: it gives complete control over the shape and quality characteristics of the dataset, and it eliminates any risk of exposing real customer or business data through an AI development tool (see Responsible AI section below).

---

## Row Count Rationale

The **10,000 : 100,000 : 500** ratio mirrors realistic e-commerce data proportions:

- **10,000 customers** — a reasonable active customer base for a focused online retailer; large enough to make segments meaningful but small enough to keep local generation fast
- **100,000 orders** — a 10:1 ratio against customers, implying each customer places roughly 10 orders over the 2020–2024 period covered by the data; this is realistic for a recurring-purchase e-commerce model
- **500 products** — a small but focused product catalogue; most boutique or category-specific e-commerce stores carry hundreds, not thousands, of SKUs

At 100,000 rows, the orders table is large enough that the 460 quality issues represent approximately 0.5% of total data — a realistic "bad data" rate that would be encountered in production. Quality issues that represent 5% or 10% of a dataset are obvious and easy to catch; 0.5% is a genuine engineering challenge.

---

## Why Faker Was Used Instead of Hardcoded Values

Hardcoded test data (e.g., `"John Smith"`, `"test@example.com"`) produces datasets that look obviously synthetic. When quality issues are planted in such a dataset — for example, a NULL email among a list of clearly fake addresses — the issue is visible at a glance rather than requiring a proper check to detect.

`Faker` generates realistic-looking names, emails, and geographic data that vary naturally across rows. This means:
- The dataset looks like real production data at first inspection
- Quality issues (like a NULL email) only surface when explicitly checked for, not just by eyeballing the data
- The pipeline is tested against realistic input, not a toy dataset

The library is seeded with `random.seed(42)`, `np.random.seed(42)`, and `Faker.seed(42)` to guarantee identical output on every run — reproducibility is a hard requirement when validation scripts depend on exact expected counts.

---

## Intentional Quality Issue Counts and Purpose

| Issue | Count | Reason for this count |
|:---|:---:|:---|
| NULL emails in customers | 50 | 0.5% of customer rows — realistic rate of missing contact data |
| Duplicate `customer_id` in customers | 10 | Small number — enough to test uniqueness detection without dominating the dataset |
| NULL `customer_id` in orders | 100 | 0.1% of orders — realistic rate of system integration failures |
| NULL `product_id` in orders | 200 | 0.2% — slightly higher, representing catalogue sync lag |
| Orphan `customer_id` in orders | 50 | Orders referencing deleted or migrated customer accounts |
| Orphan `product_id` in orders | 30 | Orders referencing discontinued products removed from catalogue |
| Duplicate `order_id` in orders | 20 | Realistic rate from double-submission bugs in order systems |
| **Total** | **460** | ~0.46% of total rows across all three files |

All 460 issues are distributed across **non-overlapping rows** — no single row carries more than one planted issue. This ensures each Silver quality check is tested independently and expected counts are unambiguous. See `src/data_generation/DATA_GENERATION_NOTES.md` for the exact partitioning logic.

---

## Responsible AI Practice — Why Real Data Was Never Used

No real customer records, production order data, or any personally identifiable information was used at any point in this project. The reasons are:

1. **AI tooling risk:** Prompts sent to AI tools may be processed, logged, or used in ways that are not fully transparent. Sharing real customer names, emails, or order histories through an AI assistant violates responsible data handling principles, regardless of the tool's stated privacy policy.

2. **No need:** Synthetic data with realistic statistical properties serves every engineering purpose this project requires — testing data ingestion, quality checks, aggregations, and dashboard visualisations. Real data adds risk without adding value.

3. **Auditability:** A synthetic dataset with a fixed seed is fully reproducible and auditable — any reviewer can regenerate the exact same data and verify pipeline outputs independently. Real data cannot offer this property.

The `Faker` library generates data that is statistically realistic but entirely fabricated — no real individual's information is represented in any of the three CSV files.
