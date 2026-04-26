import sys
from pathlib import Path

# Add the root folder to the Python path so it can find paths.py
# parents[2] = go up 2 levels from src/silver/ to root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from paths import DATA, OUT  # DATA = /data/, OUT = /out/


# LOAD FUNCTIONS
# Each function loads a CSV file and returns a DataFrame.
def load_orders():
    return pd.read_csv(
        DATA / "olist_orders_dataset.csv",
        dtype={
            "order_id": "string",      # unique order ID
            "customer_id": "string",   # customer ID
            "order_status": "category" # e.g. "delivered", "canceled" - few distinct values
        },
        parse_dates=[
            "order_purchase_timestamp",     # when was the order placed
            "order_approved_at",            # when was the payment approved
            "order_delivered_carrier_date", # when was it delivered to the courier
            "order_delivered_customer_date", # when was it delivered to the customer
            "order_estimated_delivery_date"  # promise date
        ]
    )

def load_items():
    return pd.read_csv(
        DATA / "olist_order_items_dataset.csv",
        dtype={
            "order_id": "string",   # link to orders
            "product_id": "string", # link to products
            "seller_id": "string"   # link to sellers
        },
        parse_dates=["shipping_limit_date"]  # shipping deadline from the seller
    )

def load_customers():
    return pd.read_csv(
        DATA / "olist_customers_dataset.csv",
        dtype={
            "customer_id": "string",    # link to orders
            "customer_state": "category"
        }
    )

def load_payments():
    return pd.read_csv(
        DATA / "olist_order_payments_dataset.csv",
        dtype={
            "order_id": "string",      # link to orders
            "payment_type": "category"
        }
    )

def load_products():
    return pd.read_csv(
        DATA / "olist_products_dataset.csv",
        dtype={
            "product_id": "string",            # link to items
            "product_category_name": "category"
        }
    )

def load_sellers():
    return pd.read_csv(
        DATA / "olist_sellers_dataset.csv",
        dtype={
            "seller_id": "string",    # link to items
            "seller_state": "category"
        }
    )


# MERGE
# Joins all datasets into a single DataFrame.

def merge_datasets(orders, items, customers, payments, products, sellers):
    df = items.merge(orders, on="order_id", how="left")

    df = df.merge(customers, on="customer_id", how="left")

    df = df.merge(products, on="product_id", how="left")

    df = df.merge(sellers, on="seller_id", how="left")

    payments_agg = payments.groupby("order_id").agg(
        payment_type=("payment_type", "first"),               # payment type
        payment_installments=("payment_installments", "sum"), # total installments
        payment_value=("payment_value", "sum")                # total value
    ).reset_index()

    df = df.merge(payments_agg, on="order_id", how="left")

    return df


# TRANSFORM
# Enriches the DataFrame with new computed columns.

def transform(df):

    # Time features derived from the purchase timestamp
    df["order_year"]    = df["order_purchase_timestamp"].dt.year
    df["order_month"]   = df["order_purchase_timestamp"].dt.month
    df["order_weekday"] = df["order_purchase_timestamp"].dt.weekday  # 0=Monday
    df["order_hour"]    = df["order_purchase_timestamp"].dt.hour

    # Mask: True only for fully delivered orders
    delivered_mask = (
        df["order_status"].eq("delivered") &               # status = delivered
        df["order_delivered_customer_date"].notna() &      # delivery date exists
        df["order_delivered_carrier_date"].notna() &       # courier date exists
        df["order_purchase_timestamp"].notna()             # purchase date exists
    )

    # Durations in days (only for delivered orders)
    df.loc[delivered_mask, "processing_days"] = (
        df.loc[delivered_mask, "order_delivered_carrier_date"] -
        df.loc[delivered_mask, "order_purchase_timestamp"]
    ).dt.days  # timedelta to number of days

    df.loc[delivered_mask, "shipping_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_delivered_carrier_date"]
    ).dt.days

    df.loc[delivered_mask, "fulfillment_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_purchase_timestamp"]
    ).dt.days

    # SLA: how many days early/late relative to the promised date
    df.loc[delivered_mask, "sla_diff_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_estimated_delivery_date"]
    ).dt.days

    # on_time: True if delivery was on or before the promised date
    df["on_time"] = df["sla_diff_days"] <= 0

    return df


# SAVE
# Saves the final DataFrame as a Parquet file.

def save_silver(df):
    silver_dir = OUT / "silver"
    silver_dir.mkdir(exist_ok=True)  # create folder if it does not exist

    output_path = silver_dir / "silver_orders.parquet"
    df.to_parquet(output_path, index=False)  # save without index column

    print(f"Saved silver layer: {output_path}")
    print(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
    # .shape[0] = number of rows, .shape[1] = number of columns


# MAIN
if __name__ == "__main__":
    print("Loading datasets...")
    orders    = load_orders()
    items     = load_items()
    customers = load_customers()
    payments  = load_payments()
    products  = load_products()
    sellers   = load_sellers()

    print("Merging datasets...")
    df = merge_datasets(orders, items, customers, payments, products, sellers)

    print("Transforming...")
    df = transform(df)

    print("Saving...")
    save_silver(df)