import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import pandas as pd
from azure.eventhub import EventData, EventHubProducerClient

load_dotenv()
connection_string = os.getenv("EVENT_HUB_CONNECTION_STRING")
event_hub_name = os.getenv("EVENT_HUB_NAME")

# Load real orders from Olist dataset
orders = pd.read_csv("../data/olist_orders_dataset.csv")
items = pd.read_csv("../data/olist_order_items_dataset.csv")

# Merge orders with items to get payment value
merged = orders.merge(items.groupby("order_id")["price"].sum().reset_index(), on="order_id", how="left")

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_string,
    eventhub_name=event_hub_name
)

print(f"Sending {len(merged)} orders to Event Hub...")

for _, row in merged.iterrows():
    order = {
        "order_id": row["order_id"],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": row["customer_id"],
        "status": row["order_status"],
        "total_amount": row["price"] if pd.notna(row["price"]) else 0.0,
        "purchase_timestamp": str(row["order_purchase_timestamp"])
    }

    event_json = json.dumps(order)
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(event_json))
    producer.send_batch(event_data_batch)

    print(f"Sent order: {row['order_id']}")
    time.sleep(1)  # Wait 1 second between orders to simulate real-time

producer.close()
print("All orders sent!")