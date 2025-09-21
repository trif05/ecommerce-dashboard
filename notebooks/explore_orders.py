import pandas as pd
import numpy as np

# =============================================================================
# E-COMMERCE DASHBOARD - DATA EXPLORATION
# =============================================================================

# =============================================================================
# PHASE 1: DATASET LOADING & BASIC EXPLORATION
# =============================================================================

# Φορτώνουμε το Brazilian E-commerce dataset
df = pd.read_csv(r"C:\Users\Pc User\Desktop\projects\Working_On\ecommerce-dashboard\data\olist_orders_dataset.csv")
print("=" * 60)
print("BASIC INFORMATION FOR DATASET")
print("=" * 60)

# basic dataset information
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

# =============================================================================
# PHASE 2: DATETIME ANALYSIS & FEATURE EXTRACTION
# =============================================================================

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

# =============================================================================
# BUSINESS INSIGHTS - TEMPORAL PATTERNS
# =============================================================================

print(f"\nBUSINESS INSIGHTS")
print("-" * 50)

# Orders per year
orders_per_year = df['order_year'].value_counts().sort_index()
print(f"\n Orders per year:")
for year, count in orders_per_year.items():
   print(f"   {year}: {count:,} orders")

# Peak hours of orders
hourly_distribution = df['order_hour'].value_counts().sort_index()
print(f"\n Orders per hour:")
for hour, count in hourly_distribution.head(3).items():
   print(f"   {hour:02d}:00 - {count:,} orders")

# Peak months of orders
monthly_distribution = df['order_month'].value_counts().sort_index()
print(f"\n Orders per month:")
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
for month, count in monthly_distribution.items():
   print(f"   {month_names[month-1]:4s} ({month:2d}): {count:,} orders")

print(f"\n{'=' * 60}")
print("DATA EXPLORATION COMPLETED!")
print("=" * 60)
# this code is part of the e-commerce dashboard project.