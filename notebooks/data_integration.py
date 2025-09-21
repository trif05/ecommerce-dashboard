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


#COMPARE THE DATASET SIZES BEFORE/AFTER MERGE
print("\nJOIN QUALITY METRICS")
print(f"Original orders dataset: {df_orders.shape[0]:,} rows")
print(f"Original items dataset: {df_items.shape[0]:,} rows") 
print(f"Merged dataset: {df_merged.shape[0]:,} rows")

# Check if any orders were lost in the merge
orders_lost = df_orders.shape[0] - df_merged['order_id'].nunique()
print(f"Orders lost in merge: {orders_lost}")

# Check if any items were lost
# Items in the starting dataset minus rows in merged dataset
items_lost = df_items.shape[0] - df_merged.shape[0]
print(f"Items lost in merge: {items_lost}")
#===============================================================================================================
# Revenue coverage analysis / Ανάλυση κάλυψης εσόδων
print("BUSINESS IMPACT VALIDATION")
# Calculate the revenue before merge
original_revenue = df_items['price'].sum()
# Calculate the revenue after merge
merged_revenue = df_merged['price'].sum()
revenue_coverage = (merged_revenue / original_revenue) * 100

print(f"Original total revenue: ${original_revenue:,.2f}") # Sum of all items that sold
print(f"Merged dataset revenue: ${merged_revenue:,.2f}") # Sum of all items that sold and were part of an order
print(f"Revenue coverage: {revenue_coverage:.1f}%") # This shows how much of the original revenue is covered by the merged dataset
# Customer coverage
unique_customers_merged = df_merged['customer_id'].nunique() #Avoid counting the duplicate customers
print(f"Unique customers in merged dataset: {unique_customers_merged:,}")
#===============================================================================================================
# Order complexity analysis
print("ORDER RELATIONSHIP ANALYSIS")
# Items per order distribution
items_per_order = df_merged.groupby('order_id').size()
print(f"Average items per order: {items_per_order.mean():.2f}")
print(f"Max items in single order: {items_per_order.max()}")
print(f"Orders with 1 item: {(items_per_order == 1).sum():,}")
print(f"Orders with multiple items: {(items_per_order > 1).sum():,}")

# =============================================================================
# DATA QUALITY INVESTIGATION & REMEDIATION
# =============================================================================

print("\n" + "="*60)
print("INVESTIGATING MISSING ORDERS")
print("="*60)

# Βρες ποιες παραγγελίες χάθηκαν
orders_without_items = df_orders[~df_orders['order_id'].isin(df_items['order_id'])]
print(f"Orders χωρίς items: {len(orders_without_items)}")

# Ανάλυση των missing orders
print("\nSample of missing orders:")
print(orders_without_items[['order_id', 'order_status', 'order_purchase_timestamp']].head(10))

# Τι status έχουν αυτές οι παραγγελίες;
print("\nStatus breakdown of missing orders:")
missing_status = orders_without_items['order_status'].value_counts()
print(missing_status)

# Ποσοστιαία κατανομή
print("\nPercentage breakdown:")
missing_percentage = (missing_status / len(orders_without_items)) * 100
for status, percentage in missing_percentage.items():
    print(f"{status}: {percentage:.1f}%")

#Heree we should see next time , explain the up