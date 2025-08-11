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