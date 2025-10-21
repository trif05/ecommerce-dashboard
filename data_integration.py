from pathlib import Path
import pandas as pd
import numpy as np
from paths import DATA, OUT
# =============================================================================
# DATA INTEGRATION & ANALYSIS - ORDER ITEMS ANALYSIS
# =============================================================================

# SCHEMA TYPES (What types i wait from each file)
ORDERS_DTYPES = {
    "order_id": "string",
    "customer_id": "string",
    "order_status": "category",
}
ORDERS_PARSE_DATES = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

ITEMS_DTYPES = {
    "order_id": "string",
    "product_id": "string",
    "seller_id": "string",
}
ITEMS_PARSE_DATES = ["shipping_limit_date"]

#Loaders functions
def load_orders():
    df = pd.read_csv(
        DATA / "olist_orders_dataset.csv",
        dtype=ORDERS_DTYPES,
        parse_dates=ORDERS_PARSE_DATES
    )
    return df

def load_items():
    df = pd.read_csv(
        DATA / "olist_order_items_dataset.csv",
        dtype=ITEMS_DTYPES,
        parse_dates=ITEMS_PARSE_DATES
    )
    return df

orders = load_orders()
items  = load_items()
df_merged = orders.merge(items, on="order_id", how="inner", validate="one_to_many")
df_merged.to_csv(OUT / "integrated_data.csv", index=False)
print("DATA folder:", DATA.resolve())
print("OUT folder:", OUT.resolve())
#-----------ASSERTIONS SECTION-----------------------
#Columns that must exist
assert "order_id" in orders.columns , "orders: missing 'order_id"
assert "order_id" in items.columns , "items: missing 'order_id"

#Types that we want
assert pd.api.types.is_string_dtype(orders["order_id"]), "orders.oreder_id must be string" # We check if the column is string
assert isinstance(orders["order_status"].dtype, pd.CategoricalDtype) , "orders.order_status must be category"
for c in ORDERS_PARSE_DATES:
    assert pd.api.types.is_datetime64_any_dtype(orders[c]) , f"orders.{c} must be datetime"
for c in ITEMS_PARSE_DATES:
    assert pd.api.types.is_datetime64_any_dtype(items[c]), f"items.{c} must be datetime"

# Unique order ids
assert orders["order_id"].is_unique, "orders: order_id must be unique"

# Shape of the merged dataset
rows = df_merged.shape[0]
columns= df_merged.shape[1]
preview = df_merged.head()
print(f"Total rows in merged dataset: {rows:,}")
print(f"Total columns in merged dataset: {columns}")
print(f"Preview of first 5 rows of merged dataset: {preview}")

# Colum names in the merged dataset
print("\nCOLUMNS IN THE MERGED DATASET")
columns_names= df_merged.columns.tolist()
print(columns_names)

#Check for missing values in the merged dataset
print("\nMISSING VALUES ANALYSIS")
missing_values=df_merged.isnull().sum()
print(missing_values)


#COMPARE THE DATASET SIZES BEFORE/AFTER MERGE
print("\nJOIN QUALITY METRICS")
print(f"Original orders dataset: {orders.shape[0]:,} rows")
print(f"Original items dataset: {items.shape[0]:,} rows") 
print(f"Merged dataset: {df_merged.shape[0]:,} rows")

# Check if any orders were lost in the merge
orders_lost = orders.shape[0] - df_merged['order_id'].nunique()
print(f"Orders lost in merge: {orders_lost}")

# Check if any items were lost
# Items in the starting dataset minus rows in merged dataset
items_lost = items.shape[0] - df_merged.shape[0]
print(f"Items lost in merge: {items_lost}")
#===============================================================================================================
# Revenue coverage analysis / Ανάλυση κάλυψης εσόδων
print("BUSINESS IMPACT VALIDATION")
# Calculate the revenue before merge
original_revenue = items['price'].sum()
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

# Find which orders have been lost
orders_without_items = orders[~orders['order_id'].isin(items['order_id'])]
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

#Time features for order purchase timestamp
print("\nTime features for order purchase timestamp:")
df_merged['order_purchase_timestamp'] =pd.to_datetime(df_merged['order_purchase_timestamp'],errors='coerce') #Converts string ->datetime64/if something cant be convert set as NaT
df_merged['order_year']=df_merged['order_purchase_timestamp'].dt.year
df_merged['order_month']=df_merged['order_purchase_timestamp'].dt.month
df_merged['order_weekday']=df_merged['order_purchase_timestamp'].dt.weekday
df_merged['order_hour']=df_merged['order_purchase_timestamp'].dt.hour
print(df_merged[['order_purchase_timestamp','order_year','order_month','order_weekday','order_hour']].head())

#Durations & SLA
#A boolean line with True if is delivered or False if is not
delivered_mask = (
    df_merged["order_status"].eq("delivered") & # Returns True onlu oreder status is delivered
    df_merged["order_delivered_customer_date"].notna() # Returns True onlu oreder status is not NaT
)
#I limit even more to the lines that have all the dates you need to calculate the processing time
valid_proc = (
    delivered_mask # We check if is delivered so that we dont need to calculate the canseled orders
    & df_merged["order_delivered_carrier_date"].notna() # We check if there is a date delivered at carrier
    & df_merged["order_purchase_timestamp"].notna() # We check if there is order date
)
#Processing days
# Here i calculate how many days past that the date that customer did the order until the order delivered at carrier
df_merged.loc[valid_proc, "processing_days"] = ( # We take only the True values at valid_proc
    # We calculate the difference and that return a Timedelta.That values are going to be saved at a new column proseccing days only for valid_proc values
    df_merged.loc[valid_proc, "order_delivered_carrier_date"]- df_merged.loc[valid_proc, "order_purchase_timestamp"]
).dt.days

#Shipping days
ship_mask = (
    df_merged["order_status"].eq("delivered") &
    df_merged["order_delivered_carrier_date"].notna() &
    df_merged["order_delivered_customer_date"].notna()
)
df_merged.loc[ship_mask,"shipping_days"]= (
    df_merged.loc[ship_mask,"order_delivered_customer_date"] - df_merged.loc[ship_mask,"order_delivered_carrier_date"]
).dt.days