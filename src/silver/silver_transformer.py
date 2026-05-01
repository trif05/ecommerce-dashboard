import sys
from pathlib import Path
import json
import os

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from paths import DATA, OUT

load_dotenv(Path(__file__).resolve().parents[2] / "producer" / ".env")

# LOAD FUNCTIONS
# Each function loads a CSV file and returns a DataFrame.

def load_orders():
    return pd.read_csv(
        DATA / "olist_orders_dataset.csv",
        dtype={
            "order_id": "string",
            "customer_id": "string",
            "order_status": "category"
        },
        parse_dates=[
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ]
    )

def load_items():
    return pd.read_csv(
        DATA / "olist_order_items_dataset.csv",
        dtype={
            "order_id": "string",
            "product_id": "string",
            "seller_id": "string"
        },
        parse_dates=["shipping_limit_date"]
    )

def load_customers():
    return pd.read_csv(
        DATA / "olist_customers_dataset.csv",
        dtype={
            "customer_id": "string",
            "customer_state": "category"
        }
    )

def load_payments():
    return pd.read_csv(
        DATA / "olist_order_payments_dataset.csv",
        dtype={
            "order_id": "string",
            "payment_type": "category"
        }
    )

def load_products():
    return pd.read_csv(
        DATA / "olist_products_dataset.csv",
        dtype={
            "product_id": "string",
            "product_category_name": "category"
        }
    )

def load_sellers():
    return pd.read_csv(
        DATA / "olist_sellers_dataset.csv",
        dtype={
            "seller_id": "string",
            "seller_state": "category"
        }
    )

# LOAD FROM BLOB STORAGE
# Reads all JSON files from the bronze/orders container in Azure Blob Storage.
# Each JSON file is one order event sent by the producer.
# Returns a DataFrame with all the new orders found in the blob.
# If no orders are found, returns None.

def load_bronze_from_blob():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("bronze")

    orders = []
    for blob in container_client.list_blobs(name_starts_with="orders/"):
        blob_client = container_client.get_blob_client(blob)
        content = blob_client.download_blob().readall()
        order = json.loads(content)
        orders.append(order)

    if not orders:
        print("No new orders found in Blob Storage")
        return None

    df_blob = pd.DataFrame(orders)
    print(f"Loaded {len(df_blob):,} orders from Blob Storage")
    return df_blob


# MERGE
# Joins all datasets into a single DataFrame.

def merge_datasets(orders, items, customers, payments, products, sellers):
    df = items.merge(orders, on="order_id", how="left")
    df = df.merge(customers, on="customer_id", how="left")
    df = df.merge(products, on="product_id", how="left")
    df = df.merge(sellers, on="seller_id", how="left")

    payments_agg = payments.groupby("order_id").agg(
        payment_type=("payment_type", "first"),
        payment_installments=("payment_installments", "sum"),
        payment_value=("payment_value", "sum")
    ).reset_index()

    df = df.merge(payments_agg, on="order_id", how="left")
    return df


# MERGE BLOB ORDERS
# Merges the new orders from Blob Storage into the main silver DataFrame.
# Uses pd.concat to append the new rows to the existing DataFrame.
# drop_duplicates ensures that if an order was already in the CSV
# and also sent via Event Hub, it appears only once.

def merge_blob_orders(df_silver, df_blob):

    if df_blob is None:
        return df_silver
    
    df_blob["order_id"] = df_blob["order_id"].astype("string")
    common_cols = [col for col in df_blob.columns if col in df_silver.columns]
    df_combined = pd.concat([df_silver, df_blob[common_cols]], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["order_id"])
    print(f"Combined shape: {df_combined.shape[0]:,} rows")
    return df_combined


# TRANSFORM
# Enriches the DataFrame with new computed columns.

def transform(df):
    df["order_year"]    = df["order_purchase_timestamp"].dt.year
    df["order_month"]   = df["order_purchase_timestamp"].dt.month
    df["order_weekday"] = df["order_purchase_timestamp"].dt.weekday
    df["order_hour"]    = df["order_purchase_timestamp"].dt.hour

    delivered_mask = (
        df["order_status"].eq("delivered") &
        df["order_delivered_customer_date"].notna() &
        df["order_delivered_carrier_date"].notna() &
        df["order_purchase_timestamp"].notna()
    )

    df.loc[delivered_mask, "processing_days"] = (
        df.loc[delivered_mask, "order_delivered_carrier_date"] -
        df.loc[delivered_mask, "order_purchase_timestamp"]
    ).dt.days

    df.loc[delivered_mask, "shipping_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_delivered_carrier_date"]
    ).dt.days

    df.loc[delivered_mask, "fulfillment_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_purchase_timestamp"]
    ).dt.days

    df.loc[delivered_mask, "sla_diff_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_estimated_delivery_date"]
    ).dt.days

    df["on_time"] = df["sla_diff_days"] <= 0
    return df


# SAVE
# Saves the final DataFrame as a Parquet file.

def save_silver(df):
    silver_dir = OUT / "silver"
    silver_dir.mkdir(exist_ok=True)

    output_path = silver_dir / "silver_orders.parquet"
    df.to_parquet(output_path, index=False)

    print(f"Saved silver layer: {output_path}")
    print(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")


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

    print("Loading new orders from Blob Storage...")
    df_blob = load_bronze_from_blob()
    df = merge_blob_orders(df, df_blob)

    print("Transforming...")
    df = transform(df)

    print("Saving...")
    save_silver(df)