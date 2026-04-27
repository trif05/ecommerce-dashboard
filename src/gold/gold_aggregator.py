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