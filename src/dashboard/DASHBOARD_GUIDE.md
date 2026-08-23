# Dashboard Setup Guide

**Tool:** Databricks SQL
**Source:** `src/dashboard/dashboard_queries.sql`
**Prerequisites:** Gold layer and Silver layer pipelines completed successfully

---

## Step 1 — Register Gold Delta Tables as SQL Tables

Before any dashboard query can run, the Gold Delta tables must be registered as queryable SQL tables in Databricks. Open a **Databricks SQL Query Editor** and run the following statements one at a time:

```sql
-- Register Gold tables for dashboard use
CREATE TABLE IF NOT EXISTS gold_sales_by_product
  USING DELTA
  LOCATION '/FileStore/delta/gold/gold_sales_by_product';

CREATE TABLE IF NOT EXISTS gold_revenue_by_customer
  USING DELTA
  LOCATION '/FileStore/delta/gold/gold_revenue_by_customer';

CREATE TABLE IF NOT EXISTS gold_customer_segmentation
  USING DELTA
  LOCATION '/FileStore/delta/gold/gold_customer_segmentation';

CREATE TABLE IF NOT EXISTS gold_weekly_trends
  USING DELTA
  LOCATION '/FileStore/delta/gold/gold_weekly_trends';

-- Also register silver_orders — needed for Query 5 (monthly segment trend)
CREATE TABLE IF NOT EXISTS silver_orders
  USING DELTA
  LOCATION '/FileStore/delta/silver/silver_orders';
```

Verify each table registered successfully by running:
```sql
SHOW TABLES;
```
You should see all five tables listed.

---

## Step 2 — Create a New Dashboard

1. In the left sidebar, click **Dashboards**
2. Click **Create Dashboard** (top right)
3. Name it: `E-Commerce Sales Dashboard`
4. Click **Add** → **Visualization** to add your first tile

---

## Step 3 — Add Each Query and Visualization

Work through the five queries below in order. For each one: paste the query into a new query editor, run it to confirm it returns data, then add it to the dashboard as a visualization.

---

### Query 1 — Top 10 Products by Revenue

**SQL file section:** Query 1 in `dashboard_queries.sql`

**Visualization type:** Bar Chart

| Setting | Value |
|:---|:---|
| X axis | `product_name` |
| Y axis | `total_revenue` |
| Group by / Color | `category` |
| Sort | Descending by `total_revenue` |
| Title | `Top 10 Products by Revenue` |

**Expected appearance:** A horizontal or vertical bar chart with 10 bars, each labelled with a product name and coloured by category (e.g., Electronics, Clothing, Beauty). The tallest bar is the highest-revenue product. The category colour coding allows quick visual identification of which categories dominate revenue.

**Tip:** If Databricks renders as a table by default, click the chart icon in the results panel and select **Bar**.

---

### Query 2 — Customer Revenue Distribution

**SQL file section:** Query 2 in `dashboard_queries.sql`

**Visualization type:** Bar Chart (used as a histogram)

| Setting | Value |
|:---|:---|
| X axis | `bucket_label` |
| Y axis | `customer_count` |
| Sort | Ascending by `bucket_label` (use the numbered prefix `1.`, `2.`... to ensure correct order) |
| Title | `Customer Revenue Distribution` |

**Expected appearance:** Six bars representing spend buckets from `$0–$500` through `$10,000+`. Most customers should fall in the lower buckets (Standard/Basic segments), with a long tail in the upper ranges (Premium/High-Value customers). This histogram shape confirms the realistic distribution in the synthetic data.

**Optional:** Add a second Y axis for `avg_revenue_in_bucket` as a line overlay to show average spend within each bucket.

---

### Query 3 — Customer Segmentation Breakdown

**SQL file section:** Query 3 in `dashboard_queries.sql`

**Visualization type:** Pie Chart (or Donut Chart)

| Setting | Value |
|:---|:---|
| Label column | `segment_type` |
| Value column | `customer_count` |
| Title | `Customer Segmentation Breakdown` |

**Expected appearance:** Four slices — High-Value, Repeat, One-Time, Inactive. The relative sizes reflect the real distribution produced by the segmentation script. Hovering on a slice should show the `total_revenue` and `avg_revenue` for that segment.

**Tip:** If the pie chart is hard to read with four segments, switch to a **Donut** chart with a legend — Databricks SQL supports both.

---

### Query 4 — Weekly Revenue Trend

**SQL file section:** Query 4 in `dashboard_queries.sql`

**Visualization type:** Line Chart

| Setting | Value |
|:---|:---|
| X axis | `week_number` |
| Y axis (primary) | `weekly_revenue` |
| Y axis (secondary, optional) | `ytd_revenue` (running cumulative) |
| Group by | `order_year` (if multi-year data, this splits into separate lines) |
| Title | `Weekly Revenue Trend` |

**Expected appearance:** A line that fluctuates week-by-week, showing seasonal patterns across the 2020–2024 date range of the synthetic data. The optional secondary `ytd_revenue` line shows an ever-increasing cumulative curve on the same chart, confirming the business is growing over time.

**Filter to add:** A year dropdown filter on `order_year` lets you isolate a single year's weekly trend.

---

### Query 5 — Monthly Revenue by Customer Segment

**SQL file section:** Query 5 in `dashboard_queries.sql`

**Visualization type:** Stacked Bar Chart

| Setting | Value |
|:---|:---|
| X axis | `order_month` |
| Y axis | `monthly_revenue` |
| Stack / Color by | `customer_segment` |
| Title | `Monthly Revenue by Customer Segment` |

**Expected appearance:** A bar for each month, stacked into three coloured sections (Premium, Standard, Basic). The relative height of each section shows which segment is contributing most in any given month. A growing Premium stack over time would indicate successful upselling.

**Tip:** Format the X axis as a date (`YYYY-MM`) for clean monthly labels rather than full timestamps.

---

## Step 4 — Arrange Dashboard Tiles

Recommended layout (drag and resize tiles in the Databricks dashboard editor):

```
┌─────────────────────────┬───────────────────────────┐
│  Query 1 (Bar)          │  Query 3 (Pie)            │
│  Top 10 Products        │  Segmentation Breakdown   │
│  by Revenue             │                           │
├─────────────────────────┴───────────────────────────┤
│  Query 4 (Line)                                     │
│  Weekly Revenue Trend                               │
├─────────────────────────┬───────────────────────────┤
│  Query 2 (Histogram)    │  Query 5 (Stacked Bar)    │
│  Customer Revenue       │  Monthly Revenue by       │
│  Distribution           │  Segment                  │
└─────────────────────────┴───────────────────────────┘
```

- Tiles can be resized by dragging the bottom-right corner
- Click the pencil icon on any tile to rename it
- Use the **Text** tile type to add a dashboard title or section headers

---

## Step 5 — Add a Dashboard Title Block

1. Click **Add** → **Text**
2. Paste the following:

```
## E-Commerce Sales Dashboard
Built on Databricks Medallion Architecture | Gold Layer Analytics
Data range: 2020–2024 | Source: 100,000+ orders across 10,000 customers
```

Place this text tile at the very top of the dashboard.

---

## Troubleshooting

| Problem | Likely cause | Fix |
|:---|:---|:---|
| `Table not found` error | Gold table not registered | Run the `CREATE TABLE ... USING DELTA` statements in Step 1 |
| Query 5 returns 0 rows | `silver_orders` not registered | Add the `silver_orders` `CREATE TABLE` statement |
| Chart shows as table | Visualization not selected | Click the chart icon in the query results panel |
| X axis order is wrong | Bucket labels not sorted | Query 2 uses numbered prefixes (`1.`, `2.`...) — ensure `ORDER BY bucket_label ASC` is present |
| No data in weekly trends | Gold pipeline not run | Run `src/gold/create_gold_tables.py` first |
