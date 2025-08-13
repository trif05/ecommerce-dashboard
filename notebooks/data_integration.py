import pandas as pd
import numpy as np

# =============================================================================
# DATA INTEGRATION - ORDER ITEMS ANALYSIS
# =============================================================================

# =============================================================================
# PHASE 2: DATA INTEGRATION & ANALYSIS
# =============================================================================

df_items=pd.read_csv(r"C:\Users\Pc User\Desktop\projects\Working_On\ecommerce-dashboard\data\olist_order_items_dataset.csv")
df_orders=pd.read_csv(r"C:\Users\Pc User\Desktop\projects\Working_On\ecommerce-dashboard\data\olist_orders_dataset.csv")
df_merged = df_orders.merge(df_items, on='order_id', how='inner')
df_merged.to_csv(r"C:\Users\Pc User\Desktop\projects\Working_On\ecommerce-dashboard\data\integrated_data.csv", index=False)

# Shape of the merged dataset
rows = df_merged.shape[0]
colums= df_merged.shape[1]
preview = df_merged.head()
print(f"Total rows in merged dataset: {rows:,}")
print(f"Total columns in merged dataset: {colums}")
print(f"Preview of first 5 rows of merged dataset: {preview}")

# Colum names in the merged dataset
print("\nCOLUMNS IN THE MERGED DATASET")
colums_names= df_merged.columns.tolist()
print(colums_names)

#Check for missing values in the merged dataset
print("\nMISSING VALUES ANALYSIS")
missing_values=df_merged.isnull().sum()
print(missing_values)
