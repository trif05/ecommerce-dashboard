import pandas as pd
import numpy as np

# =============================================================================
# ORDER ITEMS ANALYSIS - REVENUE & PRODUCT EXPLORATION
# =============================================================================

# =============================================================================
# PHASE 1: DATASET LOADING & BASIC EXPLORATION
# =============================================================================

print("=" * 60)
print("💰 ORDER ITEMS DATASET EXPLORATION")
print("=" * 60)

# Φορτώνουμε το order items dataset
df = pd.read_csv('/Users/thodoristrifonopoulos/Desktop/projects/ecommerce-dashboard/data/olist_order_items_dataset.csv')
# Data shape & structure
print(f"📦 Total number of order items: {df.shape[0]:,}")
print(f"📋 Number of columns: {df.shape[1]}")
print("Preview of first 5 rows:")
print(df.head())
# Type of data for each column
print(f"\n🔢 TYPE OF DATA: ")
print(df.dtypes)
# Missing values analysis
print(f"\n⚠️ MISSING VALUES ANALYSIS")
print(df.isnull().sum())