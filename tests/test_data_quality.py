import pandas as pd
import numpy as np
import os

def test_completeness_check_catches_null_emails():
    df = pd.read_csv('data/customers.csv')
    null_count = df['email'].isna().sum()
    print(f"\nChecking NULL emails: found {null_count}")
    assert null_count == 50, f"Expected exactly 50 NULL emails, got {null_count}"

def test_completeness_check_catches_null_customer_ids():
    df = pd.read_csv('data/orders.csv')
    null_count = df['customer_id'].isna().sum()
    print(f"\nChecking NULL customer_ids in orders: found {null_count}")
    assert null_count == 100, f"Expected exactly 100 NULL customer_ids, got {null_count}"

def test_completeness_check_catches_null_product_ids():
    df = pd.read_csv('data/orders.csv')
    null_count = df['product_id'].isna().sum()
    print(f"\nChecking NULL product_ids in orders: found {null_count}")
    assert null_count == 200, f"Expected exactly 200 NULL product_ids, got {null_count}"

def test_uniqueness_check_catches_duplicate_customer_ids():
    df = pd.read_csv('data/customers.csv')
    dup_count = df['customer_id'].duplicated().sum()
    print(f"\nChecking duplicate customer_ids: found {dup_count}")
    assert dup_count == 10, f"Expected exactly 10 duplicate customer_ids, got {dup_count}"

def test_uniqueness_check_catches_duplicate_order_ids():
    df = pd.read_csv('data/orders.csv')
    dup_count = df['order_id'].duplicated().sum()
    print(f"\nChecking duplicate order_ids: found {dup_count}")
    assert dup_count == 20, f"Expected exactly 20 duplicate order_ids, got {dup_count}"

def test_referential_integrity_catches_orphan_customer_ids():
    customers = pd.read_csv('data/customers.csv')
    orders = pd.read_csv('data/orders.csv')
    valid_orders = orders[orders['customer_id'].notna()]
    orphan_count = (~valid_orders['customer_id'].isin(customers['customer_id'])).sum()
    print(f"\nChecking orphan customer_ids: found {orphan_count}")
    assert orphan_count == 50, f"Expected exactly 50 orphan customer_ids, got {orphan_count}"

def test_referential_integrity_catches_orphan_product_ids():
    products = pd.read_csv('data/products.csv')
    orders = pd.read_csv('data/orders.csv')
    valid_orders = orders[orders['product_id'].notna()]
    orphan_count = (~valid_orders['product_id'].isin(products['product_id'])).sum()
    print(f"\nChecking orphan product_ids: found {orphan_count}")
    assert orphan_count == 30, f"Expected exactly 30 orphan product_ids, got {orphan_count}"

def test_products_have_no_quality_issues():
    df = pd.read_csv('data/products.csv')
    null_count = df.isna().sum().sum()
    print(f"\nChecking total NULLs in products: found {null_count}")
    assert null_count == 0, f"Expected 0 NULLs across all product columns, got {null_count}"
    
    cost_price_check = (df['cost'] < df['price']).all()
    print(f"\nChecking cost < price for all products: {cost_price_check}")
    assert cost_price_check, "Expected cost < price for every product row"

def test_total_amount_equals_quantity_times_unit_price():
    df = pd.read_csv('data/orders.csv')
    valid_df = df.dropna(subset=['quantity', 'unit_price', 'total_amount'])
    expected_amount = valid_df['quantity'] * valid_df['unit_price']
    
    diff = np.abs(valid_df['total_amount'] - expected_amount)
    pass_count = (diff <= 0.01).sum()
    total_count = len(valid_df)
    pass_rate = pass_count / total_count if total_count > 0 else 0
    
    print(f"\nChecking total_amount logic: pass rate {pass_rate*100:.2f}%")
    assert pass_rate > 0.99, f"Expected pass rate > 99%, got {pass_rate*100:.2f}%"
