import sys
from pathlib import Path

# Προσθέτουμε το root folder στο Python path ώστε να βρει το paths.py
# parents[2] = ανεβαίνουμε 2 επίπεδα πάνω από src/silver/ → root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
from paths import DATA, OUT  # DATA = /data/, OUT = /out/


# LOAD FUNCTIONS
# Κάθε συνάρτηση φορτώνει ένα CSV αρχείο και επιστρέφει ένα DataFrame.
def load_orders():
    return pd.read_csv(
        DATA / "olist_orders_dataset.csv",
        dtype={
            "order_id": "string",       # μοναδικό ID παραγγελίας
            "customer_id": "string",    # ID πελάτη
            "order_status": "category"  # π.χ. "delivered", "canceled" — λίγες τιμές
        },
        parse_dates=[
            "order_purchase_timestamp",       # πότε έγινε η παραγγελία
            "order_approved_at",              # πότε εγκρίθηκε η πληρωμή
            "order_delivered_carrier_date",   # πότε παραδόθηκε στον courier
            "order_delivered_customer_date",  # πότε παραδόθηκε στον πελάτη
            "order_estimated_delivery_date"   # πότε υποσχέθηκε να παραδοθεί
        ]
    )

def load_items():
    return pd.read_csv(
        DATA / "olist_order_items_dataset.csv",
        dtype={
            "order_id": "string",    # σύνδεσμος με orders
            "product_id": "string",  # σύνδεσμος με products
            "seller_id": "string"    # σύνδεσμος με sellers
        },
        parse_dates=["shipping_limit_date"]  # deadline αποστολής από τον seller
    )

def load_customers():
    return pd.read_csv(
        DATA / "olist_customers_dataset.csv",
        dtype={
            "customer_id": "string",       # σύνδεσμος με orders
            "customer_state": "category"   # π.χ. "SP", "RJ" — λίγες τιμές
        }
    )

def load_payments():
    return pd.read_csv(
        DATA / "olist_order_payments_dataset.csv",
        dtype={
            "order_id": "string",        # σύνδεσμος με orders
            "payment_type": "category"   # π.χ. "credit_card", "boleto"
        }
    )

def load_products():
    return pd.read_csv(
        DATA / "olist_products_dataset.csv",
        dtype={
            "product_id": "string",               # σύνδεσμος με items
            "product_category_name": "category"   # π.χ. "electronics" — λίγες τιμές
        }
    )

def load_sellers():
    return pd.read_csv(
        DATA / "olist_sellers_dataset.csv",
        dtype={
            "seller_id": "string",       # σύνδεσμος με items
            "seller_state": "category"   # π.χ. "SP", "MG" — λίγες τιμές
        }
    )


# ===========================================================================
# MERGE
# Ενώνει όλα τα datasets σε ένα ενιαίο DataFrame.
#
# Αφετηρία: items — κάθε γραμμή = ένα προϊόν που αγοράστηκε.
# Πάνω σε αυτό "κολλάμε" πληροφορίες από τα άλλα datasets με merge().
#
# .merge(other, on="key", how="left"):
#   → on="key"    : η στήλη-κλειδί που ταιριάζει rows ανάμεσα στα δύο DataFrames
#   → how="left"  : κράτα ΟΛΕΣ τις γραμμές του αριστερού (items),
#                   ακόμα και αν δεν υπάρχει αντίστοιχο row στο δεξί dataset
#                   (αντί για "inner" που θα πετούσε τις ασύζευκτες)
#
# Payments ειδική περίπτωση:
#   Μία παραγγελία μπορεί να έχει ΠΟΛΛΕΣ γραμμές πληρωμής (π.χ. 2 δόσεις).
#   Δεν μπορούμε να κάνουμε απευθείας merge → θα πολλαπλασιάζονταν οι γραμμές.
#   Λύση: groupby("order_id") + agg() → συμπτύσσουμε σε 1 γραμμή ανά order_id.
#
# .groupby("order_id"): ομαδοποιεί τις γραμμές με το ίδιο order_id
# .agg(...):            για κάθε ομάδα, εφαρμόζει συνάρτηση ανά στήλη:
#   ("payment_type", "first") → παίρνει την πρώτη τιμή της ομάδας
#   ("payment_installments", "sum") → αθροίζει τις δόσεις
#   ("payment_value", "sum") → αθροίζει το συνολικό ποσό
# .reset_index(): μετατρέπει το order_id από index ξανά σε κανονική στήλη
# ===========================================================================

def merge_datasets(orders, items, customers, payments, products, sellers):

    # Αφετηρία: items (κάθε row = ένα προϊόν)
    df = items.merge(orders, on="order_id", how="left")
    # + πληροφορίες παραγγελίας (status, timestamps)

    df = df.merge(customers, on="customer_id", how="left")
    # + πληροφορίες πελάτη (πόλη, πολιτεία)

    df = df.merge(products, on="product_id", how="left")
    # + πληροφορίες προϊόντος (κατηγορία, βάρος)

    df = df.merge(sellers, on="seller_id", how="left")
    # + πληροφορίες πωλητή (πόλη, πολιτεία)

    # Συμπτύσσουμε τις πολλαπλές πληρωμές σε 1 γραμμή ανά παραγγελία
    payments_agg = payments.groupby("order_id").agg(
        payment_type=("payment_type", "first"),           # τύπος πληρωμής
        payment_installments=("payment_installments", "sum"),  # σύνολο δόσεων
        payment_value=("payment_value", "sum")            # συνολική αξία
    ).reset_index()

    df = df.merge(payments_agg, on="order_id", how="left")
    # + πληροφορίες πληρωμής (τύπος, αξία)

    return df


# ===========================================================================
# TRANSFORM
# Εμπλουτίζει το DataFrame με νέες στήλες που υπολογίζουμε εμείς.
#
# --- TIME FEATURES ---
# .dt.year / .dt.month / .dt.weekday / .dt.hour:
#   Εξάγουμε μέρη της ημερομηνίας ώστε στο Gold layer να μπορούμε να
#   κάνουμε ανάλυση ανά μήνα, ημέρα εβδομάδας, ώρα κ.λπ.
#   weekday: 0=Δευτέρα, 6=Κυριακή
#
# --- DELIVERED MASK ---
# Υπολογίζουμε διάρκειες ΜΟΝΟ για παραγγελίες που έχουν παραδοθεί.
# Αν η παραγγελία είναι canceled/pending, οι ημερομηνίες παράδοσης
# είναι NaT (κενές) → αφαίρεση θα έδινε λάθος αποτέλεσμα ή σφάλμα.
#
# .eq("delivered")  : ισοδύναμο του == "delivered", δουλεύει καλύτερα με category
# .notna()          : ελέγχει ότι η τιμή ΔΕΝ είναι NaT/NaN (υπάρχει)
# &                 : λογικό AND — όλες οι συνθήκες πρέπει να ισχύουν
#
# --- DURATIONS (με df.loc[mask, "col"]) ---
# df.loc[mask, "col"] = ... : γράφει τιμές ΜΟΝΟ στις γραμμές όπου mask=True
#                             οι υπόλοιπες γραμμές μένουν NaN (σωστή συμπεριφορά)
#
# .dt.days : μετατρέπει timedelta (διαφορά datetime) σε αριθμό ημερών (int)
#
# processing_days  : μέρες από αγορά → παράδοση σε courier (πόσο γρήγορα ετοιμάστηκε)
# shipping_days    : μέρες από courier → πελάτη (πόσο γρήγορα ταξίδεψε)
# fulfillment_days : μέρες από αγορά → πελάτη (συνολικός χρόνος)
# sla_diff_days    : πραγματική - υποσχεμένη ημερομηνία
#                    αρνητικό = ήρθε νωρίς, θετικό = ήρθε αργά
#
# on_time : True αν sla_diff_days <= 0 (παραδόθηκε εντός του υποσχεμένου χρόνου)
#           Σημείωση: ισχύει και για NaN rows → θα βγει False εκεί (αποδεκτό)
# ===========================================================================

def transform(df):

    # --- Χρονικά χαρακτηριστικά από την ώρα αγοράς ---
    df["order_year"]    = df["order_purchase_timestamp"].dt.year
    df["order_month"]   = df["order_purchase_timestamp"].dt.month
    df["order_weekday"] = df["order_purchase_timestamp"].dt.weekday  # 0=Δευτέρα
    df["order_hour"]    = df["order_purchase_timestamp"].dt.hour

    # --- Mask: True μόνο για παραγγελίες που έχουν παραδοθεί πλήρως ---
    delivered_mask = (
        df["order_status"].eq("delivered") &               # status = delivered
        df["order_delivered_customer_date"].notna() &      # υπάρχει ημ. παράδοσης
        df["order_delivered_carrier_date"].notna() &       # υπάρχει ημ. courier
        df["order_purchase_timestamp"].notna()             # υπάρχει ημ. αγοράς
    )

    # --- Διάρκειες σε μέρες (μόνο για delivered παραγγελίες) ---
    df.loc[delivered_mask, "processing_days"] = (
        df.loc[delivered_mask, "order_delivered_carrier_date"] -
        df.loc[delivered_mask, "order_purchase_timestamp"]
    ).dt.days  # timedelta → αριθμός ημερών

    df.loc[delivered_mask, "shipping_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_delivered_carrier_date"]
    ).dt.days

    df.loc[delivered_mask, "fulfillment_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_purchase_timestamp"]
    ).dt.days

    # --- SLA: πόσες μέρες νωρίτερα/αργότερα από την υπόσχεση ---
    df.loc[delivered_mask, "sla_diff_days"] = (
        df.loc[delivered_mask, "order_delivered_customer_date"] -
        df.loc[delivered_mask, "order_estimated_delivery_date"]
    ).dt.days

    # --- on_time: True αν η παράδοση ήταν εντός ή πριν την υποσχεμένη ημερομηνία ---
    df["on_time"] = df["sla_diff_days"] <= 0

    return df


# ===========================================================================
# SAVE
# Αποθηκεύει το τελικό DataFrame ως Parquet αρχείο.
#
# silver_dir.mkdir(exist_ok=True):
#   Δημιουργεί τον φάκελο out/silver/ αν δεν υπάρχει ήδη.
#   exist_ok=True → δεν πετάει σφάλμα αν υπάρχει ήδη.
#
# .to_parquet(path, index=False):
#   Αποθηκεύει ως Parquet (compressed, columnar format).
#   index=False → δεν αποθηκεύει τον αριθμό γραμμής (0,1,2...) ως στήλη.
#   Parquet vs CSV: μικρότερο μέγεθος, γρηγορότερο διάβασμα,
#   διατηρεί dtypes (δεν χρειάζεται επαναορισμός την επόμενη φορά).
# ===========================================================================

def save_silver(df):
    silver_dir = OUT / "silver"
    silver_dir.mkdir(exist_ok=True)  # δημιουργία φακέλου αν δεν υπάρχει

    output_path = silver_dir / "silver_orders.parquet"
    df.to_parquet(output_path, index=False)  # αποθήκευση χωρίς index στήλη

    print(f"Saved silver layer: {output_path}")
    print(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
    # .shape[0] = αριθμός γραμμών, .shape[1] = αριθμός στηλών


# ===========================================================================
# MAIN
# Το σημείο εκκίνησης του script.
# if __name__ == "__main__": εκτελείται ΜΟΝΟ όταν τρέχεις απευθείας αυτό
# το αρχείο (python silver_transformer.py).
# Αν κάποιος κάνει "import silver_transformer", αυτό το block ΔΕΝ τρέχει.
# Σειρά: load → merge → transform → save
# ===========================================================================

if __name__ == "__main__":
    print("Loading datasets...")
    orders    = load_orders()
    items     = load_items()
    customers = load_customers()
    payments  = load_payments()
    products  = load_products()
    sellers   = load_sellers()

    print("Merging datasets...")
    df = merge_datasets(orders, items, customers, payments, products, sellers)

    print("Transforming...")
    df = transform(df)

    print("Saving...")
    save_silver(df)