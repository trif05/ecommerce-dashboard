import pandas as pd
import numpy as np

# =============================================================================
# E-COMMERCE DASHBOARD - DATA EXPLORATION
# =============================================================================

# =============================================================================
# PHASE 1: DATASET LOADING & BASIC EXPLORATION
# =============================================================================

# Φορτώνουμε το Brazilian E-commerce dataset
df = pd.read_csv('/Users/thodoristrifonopoulos/Desktop/projects/ecommerce-dashboard/data/olist_orders_dataset.csv')

print("=" * 60)
print("📊 ΒΑΣΙΚΕΣ ΠΛΗΡΟΦΟΡΙΕΣ DATASET")
print("=" * 60)

# Βασικές διαστάσεις του dataset
print(f"📦 Σύνολο παραγγελιών: {df.shape[0]:,}")  # :, προσθέτει κόμματα στους αριθμούς
print(f"📋 Αριθμός στηλών: {df.shape[1]}")

# Προεπισκόπηση δεδομένων - δείχνουμε τις πρώτες 5 γραμμές
print(f"\n📋 ΔΕΙΓΜΑ ΔΕΔΟΜΕΝΩΝ (Πρώτες 5 παραγγελίες)")
print("-" * 60)
print(df.head())

# Εξερεύνηση δομής dataset
print(f"\n🏷️  ΣΤΗΛΕΣ DATASET")
print("-" * 30)
for i, column in enumerate(df.columns, 1):
   print(f"{i:2d}. {column}")

# Τύποι δεδομένων κάθε στήλης
print(f"\n🔢 ΤΥΠΟΙ ΔΕΔΟΜΕΝΩΝ")
print("-" * 30)
print(df.dtypes)

# Έλεγχος για κενές τιμές (data quality check)
print(f"\n⚠️  MISSING VALUES ANALYSIS")
print("-" * 30)
missing_data = df.isnull().sum()
print(missing_data)

# =============================================================================
# PHASE 2: DATETIME ANALYSIS & FEATURE EXTRACTION
# =============================================================================

print(f"\n{'=' * 60}")
print("⏰ DATETIME ANALYSIS & FEATURE ENGINEERING")
print("=" * 60)

# Μετατροπή string ημερομηνίας σε datetime object για χειρισμό
# Από: '2017-10-02 17:04:00' (string) → datetime object
df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])

# Εξαγωγή χρήσιμων χαρακτηριστικών από την ημερομηνία
df['order_date'] = df['order_purchase_timestamp'].dt.date      # Μόνο ημερομηνία: 2017-10-02
df['order_month'] = df['order_purchase_timestamp'].dt.month    # Μήνας: 1-12
df['order_year'] = df['order_purchase_timestamp'].dt.year      # Έτος: 2016, 2017, 2018
df['order_hour'] = df['order_purchase_timestamp'].dt.hour      # Ώρα: 0-23
df['order_weekday'] = df['order_purchase_timestamp'].dt.dayofweek  # Ημέρα εβδομάδας: 0=Δευτέρα, 6=Κυριακή

# Χρονικό εύρος δεδομένων
print(f"📅 Πρώτη παραγγελία: {df['order_purchase_timestamp'].min()}")
print(f"📅 Τελευταία παραγγελία: {df['order_purchase_timestamp'].max()}")

# Διαθέσιμα έτη δεδομένων
available_years = sorted(df['order_year'].unique())
print(f"📆 Έτη με δεδομένα: {available_years}")

# =============================================================================
# BUSINESS INSIGHTS - TEMPORAL PATTERNS
# =============================================================================

print(f"\n📈 BUSINESS INSIGHTS - ΧΡΟΝΙΚΑ PATTERNS")
print("-" * 50)

# Κατανομή παραγγελιών ανά έτος (χρονολογική σειρά)
orders_per_year = df['order_year'].value_counts().sort_index()
print(f"\n🗓️  ΠΑΡΑΓΓΕΛΙΕΣ ΑΝΑ ΕΤΟΣ:")
for year, count in orders_per_year.items():
   print(f"   {year}: {count:,} παραγγελίες")

# Ανάλυση peak ωρών (πρώτες 3 ώρες για preview)
hourly_distribution = df['order_hour'].value_counts().sort_index()
print(f"\n🕐 ΚΑΤΑΝΟΜΗ ΠΑΡΑΓΓΕΛΙΩΝ ΑΝΑ ΩΡΑ (Preview - πρώτες 3 ώρες):")
for hour, count in hourly_distribution.head(3).items():
   print(f"   {hour:02d}:00 - {count:,} παραγγελίες")

# Εποχικότητα - κατανομή ανά μήνα
monthly_distribution = df['order_month'].value_counts().sort_index()
print(f"\n📊 ΕΠΟΧΙΚΟΤΗΤΑ - ΠΑΡΑΓΓΕΛΙΕΣ ΑΝΑ ΜΗΝΑ:")
month_names = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαι', 'Ιουν', 
              'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']
for month, count in monthly_distribution.items():
   print(f"   {month_names[month-1]:4s} ({month:2d}): {count:,} παραγγελίες")

print(f"\n{'=' * 60}")
print("✅ DATA EXPLORATION COMPLETED!")
print("=" * 60)