import pandas as pd
import numpy as np

# Προσθες αυτό στην αρχή του κώδικα σου (μετά τα imports)
df= pd.read_csv('/Users/thodoristrifonopoulos/Desktop/projects/ecommerce-dashboard/data/olist_orders_dataset.csv')
first_col=df['order_id'].iloc[:0]
print(first_col.head())


