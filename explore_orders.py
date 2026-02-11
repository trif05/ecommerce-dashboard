import pandas as pd
import numpy as np
from paths import DATA, OUT

# =============================================================================
# E-COMMERCE DASHBOARD - DATA EXPLORATION
# =============================================================================

# Load Brazilian E-commerce dataset
df = pd.read_csv(DATA / "olist_orders_dataset.csv")
print("=" * 60)
print("BASIC INFORMATION FOR DATASET")
print("=" * 60)

# Basic dataset information
print(f"Total orders: {df.shape[0]:,}")  # :, adds commas for readability
print(f"Number of columns: {df.shape[1]}")

# Preview of first 5 rows
print(f"\n PREVIEW OF FIRST 5 ROWS")
print("-" * 60)
print(df.head())

# Columns in the dataset
print(f"\n COLUMNS IN THE DATASET")
print("-" * 30)
for i, column in enumerate(df.columns, 1):
   print(f"{i:2d}. {column}")

# Type of data for each column
print(f"\n TYPE OF DATA")
print("-" * 30)
print(df.dtypes)

# Missing values analysis
print(f"\n MISSING VALUES ANALYSIS")
print("-" * 30)
missing_data = df.isnull().sum()
print(missing_data)

print(f"\n{'=' * 60}")
print("DATETIME ANALYSIS & FEATURE ENGINEERING")
print("=" * 60)

# Transform 'order_purchase_timestamp' to datetime
# From: '2017-10-02 17:04:00'wich is str → datetime object
df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

# Extracting date, month, year, hour, and weekday
df['order_date'] = df['order_purchase_timestamp'].dt.date      # 2017-10-02
df['order_month'] = df['order_purchase_timestamp'].dt.month    # 1-12
df['order_year'] = df['order_purchase_timestamp'].dt.year      # 2016, 2017, 2018
df['order_hour'] = df['order_purchase_timestamp'].dt.hour      # 0-23
df['order_weekday'] = df['order_purchase_timestamp'].dt.dayofweek  # 0-6 (Monday=0, Sunday=6)

# First and last order dates
print(f"First order: {df['order_purchase_timestamp'].min()}")
print(f"Last order: {df['order_purchase_timestamp'].max()}")

# Available years in the dataset
available_years = sorted(df['order_year'].unique())
print(f"Years with data: {available_years}")

print(f"\nBUSINESS INSIGHTS")
print("-" * 50)

# Orders per year
orders_per_year = df['order_year'].value_counts().sort_index()
print(f"\n Orders per year:")
for year, count in orders_per_year.items():
   print(f"{year}: {count:,} orders")

# Peak hours of orders
hourly_distribution = df['order_hour'].value_counts().nlargest(3).sort_index()
print(f"\n Orders per hour:")
for hour, count in hourly_distribution.items():
   print(f"{hour:02d}:00 - {count:,} orders")

# Peak months of orders
monthly_distribution = df['order_month'].value_counts().sort_index()
print(f"\n Orders per month:")
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
for month, count in monthly_distribution.items():
   print(f"{month_names[month-1]:4s} ({month:2d}): {count:,} orders")

# Order status distribution / We check the values of order_status to understand the order fulfillment process.
print("\n Order status distribution:")
status_dist = df['order_status'].value_counts(dropna=False)
print(status_dist)

cancel_rate = df['order_status'].eq('canceled').mean() * 100
print(f"Cancel rate: {cancel_rate:.2f}%")

# Time to delivery (parse related dates safely)
print("\n TIME TO DELIVERY ANALYSIS")

for col in [
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]:
    df[col] = pd.to_datetime(df[col], errors="coerce")  # NaT if parsing fails

# Core durations (to days)
df["processing_days"]  = (df["order_delivered_carrier_date"] - df["order_purchase_timestamp"]).dt.days
df["shipping_days"]    = (df["order_delivered_customer_date"] - df["order_delivered_carrier_date"]).dt.days
df["fulfillment_days"] = (df["order_delivered_customer_date"] - df["order_purchase_timestamp"]).dt.days  # end-to-end

# SLA: promised vs actual
df["sla_diff_days"] = (df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]).dt.days
df["on_time"] = df["sla_diff_days"] <= 0   # True αν παραδόθηκε στην ώρα του ή νωρίτερα

# KPIs
delivered_mask = df["order_delivered_customer_date"].notna() # Return True if the order was delivered

kpis = {
    "orders_total":           int(len(df)), # Total orders
    "orders_delivered":       int(delivered_mask.sum()), # Total delivered orders
    "on_time_rate_%":         round(100 * df.loc[delivered_mask,"on_time"].mean(), 2), # % percentage of on-time deliveries
    "processing_days_avg":    round(df["processing_days"].mean(skipna=True), 2), # % average processing time (days)
    "shipping_days_avg":      round(df["shipping_days"].mean(skipna=True), 2), # % average shipping time (days)
    "fulfillment_days_avg":   round(df["fulfillment_days"].mean(skipna=True), 2),# % average fulfillment time (days)
    "sla_delay_rate_%":       round(100 * (df.loc[delivered_mask,"sla_diff_days"] > 0).mean(), 2) # % percentage of late deliveries
}
print("\n DELIVERY & SLA KPIs")
for k, v in kpis.items():
    print(f"{k} : {v}")

# Time series analysis (daily orders, deliveries, on-time rate)
# Resample by day, count orders, fill missing days with 0
daily_orders = (
    df.set_index("order_purchase_timestamp") # Set datetime as index
      .resample("D")["order_id"].count() # Count orders per day
      .rename("orders") # Rename series
      .fillna(0).astype(int) # Fill missing days with 0
)
# Delivered orders on timeline 
deliv = df[delivered_mask].set_index("order_delivered_customer_date").sort_index()
# Ηοw many orders were delivered each day
daily_delivered    = deliv.resample("D")["order_id"].count().rename("delivered").fillna(0).astype(int)
# On-time delivery rate per day
daily_on_time_rate = (deliv.resample("D")["on_time"].mean() * 100).rename("on_time_rate_%").fillna(0)

sample = pd.concat([daily_orders, daily_delivered, daily_on_time_rate], axis=1)
sample = sample.dropna()  # Drop NaN values for cleaner display
print("\nSample daily series:")
print(sample.sort_index().head(3))
