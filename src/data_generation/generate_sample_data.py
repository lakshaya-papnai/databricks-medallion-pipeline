import os
import random
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

def main():
    # 1. Initialization and Setup
    # Set seed for reproducibility as requested
    random.seed(42)
    np.random.seed(42)
    Faker.seed(42)
    fake = Faker()

    # Ensure the target directory exists
    os.makedirs('data', exist_ok=True)

    print("Generating products.csv...")
    # ---------------------------------------------------------
    # 2. Generate Products (No intentional quality issues)
    # ---------------------------------------------------------
    adjectives = ['Wireless', 'Smart', 'Portable', 'Cotton', 'Leather', 'Steel', 'Wooden', 'Digital', 'Classic', 'Modern', 'Ergonomic', 'Luxury']
    nouns = ['Headphones', 'T-Shirt', 'Watch', 'Speaker', 'Bottle', 'Desk', 'Chair', 'Camera', 'Bag', 'Shoes', 'Mouse', 'Keyboard']
    categories = ['Electronics', 'Clothing', 'Home & Kitchen', 'Sports', 'Books', 'Beauty', 'Toys']
    
    products_data = []
    for i in range(1, 501):
        price = round(random.uniform(5.00, 500.00), 2)
        # Cost is calculated as price * random factor between 0.3 and 0.7
        cost = round(price * random.uniform(0.3, 0.7), 2)
        
        products_data.append({
            'product_id': i,
            'product_name': f"{random.choice(adjectives)} {random.choice(nouns)}",
            'category': random.choice(categories),
            'price': price,
            'cost': cost,
            'stock_quantity': random.randint(0, 1000),
            'reorder_level': random.randint(10, 100)
        })
        
    df_products = pd.DataFrame(products_data)


    print("Generating customers.csv...")
    # ---------------------------------------------------------
    # 3. Generate Customers
    # ---------------------------------------------------------
    countries = ['India', 'USA', 'UK', 'Germany', 'France', 'Canada', 'Australia']
    segments = ['Premium', 'Standard', 'Basic']
    segment_weights = [0.2, 0.5, 0.3]
    
    customers_data = []
    for i in range(1, 10001):
        customers_data.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'email': fake.email(),
            'country': random.choices(countries)[0],
            'signup_date': fake.date_between(start_date=datetime(2020, 1, 1), end_date=datetime(2024, 12, 31)),
            'customer_segment': random.choices(segments, weights=segment_weights)[0],
            'lifetime_value': round(random.uniform(100.00, 50000.00), 2)
        })
        
    df_customers = pd.DataFrame(customers_data)

    # --- Plant Intentional Quality Issues for Customers ---
    # Issue 1: Exactly 50 NULL emails
    null_email_indices = random.sample(range(10000), 50)
    df_customers.loc[null_email_indices, 'email'] = None

    # Issue 2: Exactly 10 duplicate customer_ids
    # Select 10 existing rows and append them to the end of the dataframe
    dup_customers = df_customers.sample(n=10, replace=False, random_state=42)
    df_customers = pd.concat([df_customers, dup_customers], ignore_index=True)


    print("Generating orders.csv...")
    # ---------------------------------------------------------
    # 4. Generate Orders
    # ---------------------------------------------------------
    num_orders = 100000
    order_statuses = ['Pending', 'Completed', 'Cancelled']
    status_weights = [0.2, 0.6, 0.2]
    
    # Pre-generate dates to speed up dataframe construction
    order_dates = [fake.date_between(start_date=datetime(2022, 1, 1), end_date=datetime(2024, 12, 31)) for _ in range(num_orders)]
    
    df_orders = pd.DataFrame({
        'order_id': range(1, num_orders + 1),
        'customer_id': random.choices(range(1, 10001), k=num_orders),
        'order_date': order_dates,
        'product_id': random.choices(range(1, 501), k=num_orders),
        'quantity': random.choices(range(1, 11), k=num_orders),
        'order_status': random.choices(order_statuses, weights=status_weights, k=num_orders)
    })
    
    # Map unit_price from the products table
    price_map = df_products.set_index('product_id')['price'].to_dict()
    df_orders['unit_price'] = df_orders['product_id'].map(price_map)
    df_orders['total_amount'] = (df_orders['quantity'] * df_orders['unit_price']).round(2)
    
    # Calculate payment_date for Completed orders (order_date + 1-7 days)
    df_orders['order_date_dt'] = pd.to_datetime(df_orders['order_date'])
    random_days = pd.to_timedelta(np.random.randint(1, 8, size=num_orders), unit='d')
    df_orders['payment_date'] = df_orders['order_date_dt'] + random_days
    
    # If not Completed, set payment_date to NaT (which becomes NULL in CSV)
    df_orders.loc[df_orders['order_status'] != 'Completed', 'payment_date'] = pd.NaT
    df_orders['payment_date'] = df_orders['payment_date'].dt.date
    
    # Drop the temporary datetime column
    df_orders.drop(columns=['order_date_dt'], inplace=True)
    
    # Enforce correct column order
    df_orders = df_orders[['order_id', 'customer_id', 'order_date', 'product_id', 'quantity', 'unit_price', 'total_amount', 'order_status', 'payment_date']]

    # We use Int64 dtype to properly support integer columns with missing values (pd.NA)
    df_orders['customer_id'] = df_orders['customer_id'].astype('Int64')
    df_orders['product_id'] = df_orders['product_id'].astype('Int64')

    # --- Plant Intentional Quality Issues for Orders ---
    # We must ensure no row receives more than one issue.
    # Total unique rows needed = 100 + 200 + 50 + 30 + 20 = 400
    issue_indices = random.sample(range(num_orders), 400)
    
    null_cust_idx = issue_indices[0:100]
    null_prod_idx = issue_indices[100:300]
    orphan_cust_idx = issue_indices[300:350]
    orphan_prod_idx = issue_indices[350:380]
    dup_order_idx = issue_indices[380:400]
    
    # Issue 1: Exactly 100 NULL customer_ids
    df_orders.loc[null_cust_idx, 'customer_id'] = pd.NA
    
    # Issue 2: Exactly 200 NULL product_ids
    df_orders.loc[null_prod_idx, 'product_id'] = pd.NA
    
    # Issue 3: Exactly 50 orphan customer_ids (not in customers table)
    df_orders.loc[orphan_cust_idx, 'customer_id'] = random.choices(range(10001, 10051), k=50)
    
    # Issue 4: Exactly 30 orphan product_ids (not in products table)
    df_orders.loc[orphan_prod_idx, 'product_id'] = random.choices(range(501, 531), k=30)
    
    # Issue 5: Exactly 20 duplicate order_ids
    # Extract these rows, and append them back to create duplicates
    dup_orders = df_orders.loc[dup_order_idx].copy()
    df_orders = pd.concat([df_orders, dup_orders], ignore_index=True)


    # ---------------------------------------------------------
    # 5. Save Data to CSV
    # ---------------------------------------------------------
    print("Saving files to data/ folder...")
    df_products.to_csv('data/products.csv', index=False)
    df_customers.to_csv('data/customers.csv', index=False)
    df_orders.to_csv('data/orders.csv', index=False)

    # ---------------------------------------------------------
    # 6. Print Summary
    # ---------------------------------------------------------
    print("\n--- Data Generation Summary ---")
    print(f"Products file saved:  data/products.csv  | Total rows: {len(df_products)}")
    print(f"Customers file saved: data/customers.csv | Total rows: {len(df_customers)}")
    print(f"Orders file saved:    data/orders.csv    | Total rows: {len(df_orders)}")
    
    print("\n--- Intentional Data Quality Issues Planted ---")
    print("Customers:")
    print(f"  - NULL emails: 50")
    print(f"  - Duplicate customer_ids: 10")
    print("Orders:")
    print(f"  - NULL customer_ids: 100")
    print(f"  - NULL product_ids: 200")
    print(f"  - Orphan customer_ids (not in customers): 50")
    print(f"  - Orphan product_ids (not in products): 30")
    print(f"  - Duplicate order_ids: 20")
    print("\nAll files successfully saved in the data/ folder.")

if __name__ == "__main__":
    main()
