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

def gold_delivery_performance(delivered_df):
    #Filter only delivered orders

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


# Top Categories
# Count total orders and revenue by product category.

def gold_top_categories(df):
    # We remove nan values from the product_category_name
    categories_df = df.dropna(subset=["product_category_name"])

    result = (
        categories_df.groupby("product_category_name")
        .agg(
            total_orders=("order_id", "nunique"),   # unique orders
            total_revenue=("payment_value", "sum")  # total revenue
        )
        .reset_index()
    )

    result["total_revenue"] = result["total_revenue"].round(2)
    result = result.sort_values("total_revenue", ascending=False).reset_index(drop=True) #Sorting

    return result


# Seller Performance
# Count total orders, revenue, fulfillment days, and on-time rate by seller.

def gold_seller_performance(delivered_df):

    result = (
        delivered_df.groupby(["seller_id", "seller_state"])
        .agg(
            total_orders=("order_id", "nunique"),          # unique orders
            total_revenue=("payment_value", "sum"),        # total revenue
            avg_fulfillment_days=("fulfillment_days", "mean"),  # avg fulfillment days
            on_time_rate=("on_time", "mean")               # on-time delivery rate
        )
        .reset_index()
    )

    result["total_revenue"]         = result["total_revenue"].round(2)
    result["avg_fulfillment_days"]  = result["avg_fulfillment_days"].round(1)
    result["on_time_rate"]          = (result["on_time_rate"] * 100).round(1)

    # Sorting by highest revenue first
    result = result.sort_values("total_revenue", ascending=False).reset_index(drop=True)

    return result


# Customer Geography
# Count total orders and revenue by customer state.

def gold_customer_geography(df):
    result = (
        df.groupby("customer_state")
        .agg(
            total_orders=("order_id", "nunique"),   # unique orders by state
            total_revenue=("payment_value", "sum")  # total revenue
        )
        .reset_index()
    )

    result["total_revenue"] = result["total_revenue"].round(2)

    # Sorting by highest orders first
    result = result.sort_values("total_orders", ascending=False).reset_index(drop=True)

    return result


# SAVE
# Saving each gold dataset as a separate parquet file in the "gold" folder.
# It creates the "gold" folder if it doesn't exist and saves each DataFrame with a descriptive name.

def save_gold(datasets: dict):
    gold_dir = OUT / "gold"
    gold_dir.mkdir(exist_ok=True)  # File creation if it doesn't exist

    for name, df in datasets.items():
        output_path = gold_dir / f"{name}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"Saved: {output_path} — {df.shape[0]:,} rows x {df.shape[1]} columns")

# Main

if __name__ == "__main__":
    print("Loading silver...")
    df = load_silver()
    delivered_df = df[df["order_status"].eq("delivered")]

    print("Building gold datasets...")
    datasets = {
        "gold_sales_overview":       gold_sales_overview(df),
        "gold_delivery_performance": gold_delivery_performance(delivered_df),
        "gold_top_categories":       gold_top_categories(df),
        "gold_seller_performance":   gold_seller_performance(delivered_df),
        "gold_customer_geography":   gold_customer_geography(df)
    }

    print("Saving...")
    save_gold(datasets)
    print("Gold layer complete!")
