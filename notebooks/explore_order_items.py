import pandas as pd
import numpy as np

# =============================================================================
# ORDER ITEMS ANALYSIS - REVENUE & PRODUCT EXPLORATION
# =============================================================================

# =============================================================================
# PHASE 1: DATASET LOADING & BASIC EXPLORATION
# =============================================================================
# (/) after english comments it means that the code is translated to greek for personal understanding.

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

#Duplicate rows analysis
print(f"\nDUPLICATE ROWS ANALYSIS")
duplicate_rows = df.duplicated().sum()
if duplicate_rows > 0:
    print(f"Duplicate rows found: {duplicate_rows}")
else:
    print("No duplicate rows found.")

# Check for invalid item ids (negative or zero)
invalid_item_ids=df[df['order_id']<0]
if invalid_item_ids.empty:
    print("No invalid order ids found.")
else:
    print(f"Invalid order ids found: {len(invalid_item_ids)}")
    print(invalid_item_ids)

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

#Shipping Analysis
basic_shipping_stats = df[['freight_value']].describe()
print("\nBASIC SHIPPING STATISTICS:")
print(basic_shipping_stats)

# Most expensive shipping costs
most_expensive_shipping =df.groupby('order_id')['freight_value'].sum().sort_values(ascending=False).head(5)
print("\nMOST EXPENSIVE SHIPPING COSTS:")
print(most_expensive_shipping)

# Correlation analysis / Όταν η μία μεταβλητή αυξάνεται, τι κάνει η άλλη;
correlation = df['price'].corr(df['freight_value']) # .corr() calculates the correlation coefficient
print(f"\nPRICE vs FREIGHT CORRELATION: {correlation:.3f}")

# Average shipping percentage of product value
df['shipping_percentage'] = (df['freight_value'] / df['price']) * 100
avg_shipping_percentage = df['shipping_percentage'].mean()
print(f"SHIPPING AS % OF PRODUCT VALUE: {avg_shipping_percentage:.1f}%") # By average, shipping costs are 1/3 of product price

# Items shipping with > 50% of product price
high_shipping_items = df[df['shipping_percentage'] > 50]
print(f"Items with shipping >50% of price: {len(high_shipping_items)}")

# Top sellers by revenue / Average revenue per seller
top_sellers_revenue=df.groupby('seller_id')['price'].sum().sort_values(ascending=False).head(10)
print("TOP 10 SELLERS BY REVENUE:")
print(top_sellers_revenue)

# Wich seller sells the most / Sum of items sold per seller
top_sellers=df.groupby('seller_id')['order_item_id'].count().sort_values(ascending=False).head(10)
print("TOP 10 SELLERS BY NUMBER OF ITEMS SOLD:")
print(top_sellers)

# Seller performance analysis
seller_efficiency = top_sellers_revenue / top_sellers
seller_efficiency_clean = seller_efficiency.dropna().sort_values(ascending=False)
print("SELLER EFFICIENCY (Revenue per item):")
print(seller_efficiency_clean.sort_values(ascending=False))

# Outlier Detection / Ανιχνευση Ακραίων Τιμών
outliers_description = df[['price', 'freight_value']].describe()
print("\nOUTLIER DETECTION DESCRIPTION:")
print(outliers_description)

# Products with price > 3x mean price
outlier_products = df[df["price"] > 3 * df["price"].mean()]
print("\nProducts with price > 3x mean:")
print(outlier_products)

# Freight value outliers / Ανιχνευση Ακραίων Τιμών Μεταφορικών
freight_outliers = df[df["freight_value"] > 3 * df["freight_value"].mean()]
print("\nFreight value outliers:")
print(freight_outliers)

# Products with price > 1000€
expensive_products = df[df["price"] > 1000]
print("\nProducts with price > 1000€:")
print(expensive_products)

# Simple metrics only
summary = {
    "total_revenue": df['price'].sum(),
    "total_orders": df['order_id'].nunique(),
    "total_items": len(df),
    "total_sellers": df['seller_id'].nunique(),
    "unique_products": df['product_id'].nunique(),
    "average_order_value": df['price'].sum() / df['order_id'].nunique(),
    "average_item_price": df['price'].mean(),
    "average_freight_value": df['freight_value'].mean(),
    "average_items_per_order": df.groupby('order_id')['product_id'].count().mean(),
    "average_shipping_percentage": df['shipping_percentage'].mean(),
    "items_with_free_shipping": (df['freight_value'] == 0).sum(),
    "high_shipping_items": (df['shipping_percentage'] > 50).sum(),
    "price_freight_correlation": df['price'].corr(df['freight_value']),
    "most_expensive_item": df['price'].max(),
    "cheapest_item": df['price'].min()
}

# Separate prints για complex analysis
print("Simple Metrics Summary:")
summary_df = pd.DataFrame([summary])
print(summary_df.T)  # Transpose για καλύτερη εμφάνιση