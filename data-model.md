# Data Model

This document describes the data model across all three layers of the Databricks Medallion Architecture data pipeline.

## 1. Source Layer (Raw CSVs)

This layer represents the raw data as it is received from upstream systems.

**Relationships:**
*   `orders.customer_id` → `customers.customer_id` (Foreign Key)
*   `orders.product_id` → `products.product_id` (Foreign Key)

### customers.csv
| Column Name | Data Type | Key Type |
| :--- | :--- | :--- |
| `customer_id` | INT | Primary Key |
| `customer_name` | STRING | |
| `email` | STRING | |
| `country` | STRING | |
| `signup_date` | DATE | |
| `customer_segment` | STRING | |
| `lifetime_value` | DECIMAL | |

### orders.csv
| Column Name | Data Type | Key Type |
| :--- | :--- | :--- |
| `order_id` | INT | Primary Key |
| `customer_id` | INT | Foreign Key |
| `order_date` | DATE | |
| `product_id` | INT | Foreign Key |
| `quantity` | INT | |
| `unit_price` | DECIMAL | |
| `total_amount` | DECIMAL | |
| `order_status` | STRING | |
| `payment_date` | DATE | |

### products.csv
| Column Name | Data Type | Key Type |
| :--- | :--- | :--- |
| `product_id` | INT | Primary Key |
| `product_name` | STRING | |
| `category` | STRING | |
| `price` | DECIMAL | |
| `cost` | DECIMAL | |
| `stock_quantity` | INT | |
| `reorder_level` | INT | |

## 2. Bronze Layer

The Bronze layer ingests data exactly as it arrives into Delta tables, appending two metadata columns for tracking and lineage.

### bronze_customers
| Column Name | Data Type |
| :--- | :--- |
| `customer_id` | INT |
| `customer_name` | STRING |
| `email` | STRING |
| `country` | STRING |
| `signup_date` | DATE |
| `customer_segment` | STRING |
| `lifetime_value` | DECIMAL |
| `ingestion_timestamp` | TIMESTAMP |
| `source_file_name` | STRING |

### bronze_orders
| Column Name | Data Type |
| :--- | :--- |
| `order_id` | INT |
| `customer_id` | INT |
| `order_date` | DATE |
| `product_id` | INT |
| `quantity` | INT |
| `unit_price` | DECIMAL |
| `total_amount` | DECIMAL |
| `order_status` | STRING |
| `payment_date` | DATE |
| `ingestion_timestamp` | TIMESTAMP |
| `source_file_name` | STRING |

### bronze_products
| Column Name | Data Type |
| :--- | :--- |
| `product_id` | INT |
| `product_name` | STRING |
| `category` | STRING |
| `price` | DECIMAL |
| `cost` | DECIMAL |
| `stock_quantity` | INT |
| `reorder_level` | INT |
| `ingestion_timestamp` | TIMESTAMP |
| `source_file_name` | STRING |

## 3. Silver Layer

The Silver layer reads exclusively from the Bronze Delta tables. It performs data quality checks and appends a `quality_check_result` column indicating whether a row passed or failed specific criteria. 

### silver_customers
| Column Name | Data Type |
| :--- | :--- |
| `customer_id` | INT |
| `customer_name` | STRING |
| `email` | STRING |
| `country` | STRING |
| `signup_date` | DATE |
| `customer_segment` | STRING |
| `lifetime_value` | DECIMAL |
| `ingestion_timestamp` | TIMESTAMP |
| `source_file_name` | STRING |
| `quality_check_result` | STRING |

### silver_orders
| Column Name | Data Type |
| :--- | :--- |
| `order_id` | INT |
| `customer_id` | INT |
| `order_date` | DATE |
| `product_id` | INT |
| `quantity` | INT |
| `unit_price` | DECIMAL |
| `total_amount` | DECIMAL |
| `order_status` | STRING |
| `payment_date` | DATE |
| `ingestion_timestamp` | TIMESTAMP |
| `source_file_name` | STRING |
| `quality_check_result` | STRING |

### silver_products
| Column Name | Data Type |
| :--- | :--- |
| `product_id` | INT |
| `product_name` | STRING |
| `category` | STRING |
| `price` | DECIMAL |
| `cost` | DECIMAL |
| `stock_quantity` | INT |
| `reorder_level` | INT |
| `ingestion_timestamp` | TIMESTAMP |
| `source_file_name` | STRING |
| `quality_check_result` | STRING |

## 4. Gold Layer

The Gold layer contains heavily aggregated, business-level views optimized for reporting and dashboarding. Gold tables read only from Silver Delta tables.

### gold_sales_by_product
| Column Name | Data Type |
| :--- | :--- |
| `product_id` | INT |
| `product_name` | STRING |
| `category` | STRING |
| `total_orders` | BIGINT |
| `total_revenue` | DECIMAL |
| `avg_order_value` | DECIMAL |

### gold_revenue_by_customer
| Column Name | Data Type |
| :--- | :--- |
| `customer_id` | INT |
| `customer_name` | STRING |
| `customer_segment` | STRING |
| `total_orders` | BIGINT |
| `total_revenue` | DECIMAL |
| `avg_order_value` | DECIMAL |
| `lifetime_value_actual` | DECIMAL |

### gold_customer_segmentation
| Column Name | Data Type |
| :--- | :--- |
| `segment_type` | STRING |
| `customer_count` | BIGINT |
| `avg_revenue` | DECIMAL |
| `total_revenue` | DECIMAL |

**Segmentation Logic for `segment_type`:**
*   **High-Value:** Top 20% of customers by revenue
*   **Repeat:** 2+ completed orders
*   **One-Time:** Exactly 1 completed order
*   **Inactive:** 0 completed orders

## 5. Data Lineage Summary

| Source Entity | Source CSV | Bronze Table | Silver Table | Gold Tables Supported |
| :--- | :--- | :--- | :--- | :--- |
| **Customers** | `customers.csv` | `bronze_customers` | `silver_customers` | `gold_revenue_by_customer`, `gold_customer_segmentation` |
| **Orders** | `orders.csv` | `bronze_orders` | `silver_orders` | All Gold Tables |
| **Products** | `products.csv` | `bronze_products` | `silver_products` | `gold_sales_by_product` |
