import pandas as pd
import numpy as np

# =============================================================================
# ORDER ITEMS ANALYSIS - REVENUE & PRODUCT EXPLORATION
# =============================================================================

# =============================================================================
# PHASE 1: DATASET LOADING & BASIC EXPLORATION
# =============================================================================

print("=" * 60)
print("ORDER ITEMS DATASET EXPLORATION")
print("=" * 60)

# Φορτώνουμε το order items dataset
df = pd.read_csv(r"C:\Users\Pc User\Desktop\projects\Working_On\ecommerce-dashboard\data\olist_order_items_dataset.csv")
# Data shape & structure
print(f"Total number of order items: {df.shape[0]:,}")
print(f"Number of columns: {df.shape[1]}")
print("Preview of first 5 rows:")
print(df.head())
# Type of data for each column
print(f"\nTYPE OF DATA: ")
print(df.dtypes)
# Missing values analysis
print(f"\nMISSING VALUES ANALYSIS")
print(df.isnull().sum())

#Descriptive statistics for numerical columns(price,freight_value,order_item_id)
stats=df[['price', 'freight_value', 'order_item_id']].describe()
print("\nDESCRIPTIVE STATISTICS:")
print(stats)