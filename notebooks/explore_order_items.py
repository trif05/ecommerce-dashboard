import pandas as pd
import numpy as np

# =============================================================================
# ORDER ITEMS ANALYSIS - REVENUE & PRODUCT EXPLORATION
# =============================================================================

# =============================================================================
# PHASE 1: DATASET LOADING & BASIC EXPLORATION
# =============================================================================
# (/) after english comments it means that the code is translated to greek for personal understanding.
#len(df) =  112,650

print("=" * 60)
print("ORDER ITEMS DATASET EXPLORATION")
print("=" * 60)

# Load the order items dataset
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

# Unique values in order_item_id , product_id.
# With unique order we can understand how many distinct orders and products are present.
print("\n Order Behavior:")
unique_orders = df['order_id'].nunique()
unique_products = df['product_id'].nunique()
print(f"\nUnique orders: {unique_orders:,}")
print(f"Unique products: {unique_products:,}")

# Average items per order and product popularity
items_per_order = len(df) / unique_orders #Average items per order | 112,650 / 98,666 = 1.14
product_popularity = len(df) / unique_products #Product popularity
print(f"Average items per order: {items_per_order:.2f}")
print(f"Average sales per product: {product_popularity:.1f}")

# Total revenue and average price per item /revenue = χργηματα που εισεπραξε το marketplace
total_revenue = df['price'].sum()
print(f"Total Revenue: {total_revenue:,}€")

# Average Order Value / Mo Αξία Παραγγελίας
aov= total_revenue / unique_orders
print(f"Average Order Value (AOV): {aov:.2f}€")

# Top 10 best selling products /Ascending = Ανερχομενη σειρα/
top_products = df.groupby('product_id')['price'].count().sort_values(ascending=False).head(10)
print("TOP 10 BEST SELLING PRODUCTS:")
print(top_products)

# Top 10 products by revenue
highest_revenue_products=df.groupby('product_id')['price'].sum().sort_values(ascending=False).head(10)
print("TOP 10 HIGHEST REVENUE PRODUCTS:")
print(highest_revenue_products)

# Top 10 products with highest average price. 
# Usage : price consistency analysis ,price consistency analysis,pricing strategy insights,Outlier detection (products with weird pricing).
avg_price_products = df.groupby('product_id')['price'].mean().sort_values(ascending=False).head(10)
print("10 HIGHEST AVERAGE PRICE PRODUCTS:")
print(avg_price_products)

# How much items an order has, on average
items_per_order = df.groupby('order_id')['product_id'].count()  # Count of items per order
average_items = items_per_order.mean() # Average items per order
print(f"AVERAGE ITEMS PER ORDER: {average_items:.2f}")
biggest_orders = items_per_order.sort_values(ascending=False).head(10)


# Distribution Analysis
order_size_distribution = items_per_order.value_counts().sort_index()
print("ORDER SIZE DISTRIBUTION:")
print(order_size_distribution.head(10))

# Percentage Breakdown
# % of single-item orders
single_item_orders = (items_per_order == 1).sum()
total_orders = len(items_per_order)
single_item_percentage = (single_item_orders / total_orders) * 100
print(f"SINGLE ITEM ORDERS: {single_item_percentage:.1f}%")
# % of multi-item orders
multi_item_orders = 100 - single_item_percentage
print(f"MULTI ITEM ORDERS: {multi_item_orders:.1f}%")

# Extreme Orders
max_items = items_per_order.max() #Initializing at line 45. Overited at line 76
print(f"LARGEST ORDER ITEMS: {max_items} items")