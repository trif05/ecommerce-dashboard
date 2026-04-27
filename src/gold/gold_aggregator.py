import sys
from pathlib import Path

# Add the root folder to the Python path so it can find paths.py
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from paths import OUT

# Load Silver Data
# We are loading the silver parquet file 

def load_silver():
    silver_path = OUT / "silver" / "silver_orders.parquet"
    df = pd.read_parquet(silver_path)
    print(f"Loaded silver: {df.shape[0]:,} rows x {df.shape[1]} columns")
    return df

# Sales Overview
# Group by year and month, count unique orders and sum revenue.

def gold_sales_overview(df):
    result = (
        df.groupby(["order_year", "order_month"])
        .agg(
            total_orders=("order_id", "nunique"),    # unique orders
            total_revenue=("payment_value", "sum")   # sum revenue
        )
        .reset_index()
    )
    result["total_revenue"] = result["total_revenue"].round(2)
    result["avg_order_value"] = (result["total_revenue"] / result["total_orders"]).round(2)

    return result

# Delivery Performance
# Count the delivery performance by year and month.

def gold_delivery_performance(df):
    #Filter only delivered orders
    delivered_df = df[df["order_status"].eq("delivered")]

    result = (
        delivered_df.groupby(["order_year", "order_month"])
        .agg(
            avg_fulfillment_days=("fulfillment_days", "mean"),
            avg_shipping_days=("shipping_days", "mean"),
            on_time_rate=("on_time", "mean")
        )
        .reset_index()
    )

    result["avg_fulfillment_days"] = result["avg_fulfillment_days"].round(1)
    result["avg_shipping_days"]    = result["avg_shipping_days"].round(1)
    result["on_time_rate"]         = (result["on_time_rate"] * 100).round(1)

    return result

