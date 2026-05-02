import os
import json
import time
import uuid
import random
import threading
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from azure.eventhub import EventData, EventHubProducerClient

load_dotenv()
connection_string = os.getenv("EVENT_HUB_CONNECTION_STRING")
event_hub_name = os.getenv("EVENT_HUB_NAME")

STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "ES", "PE",
          "CE", "MA", "MT", "MS", "DF", "PB", "AM", "RN", "AL", "PA"]

CATEGORIES = ["cama_mesa_banho", "beleza_saude", "informatica_acessorios",
              "moveis_decoracao", "esporte_lazer", "utilidades_domesticas",
              "relogios_presentes", "ferramentas_jardim", "cool_stuff", "automotivo"]

SELLERS = {
    "SP": ["seller_sp_1", "seller_sp_2", "seller_sp_3"],
    "RJ": ["seller_rj_1", "seller_rj_2"],
    "MG": ["seller_mg_1", "seller_mg_2"],
    "PR": ["seller_pr_1"],
    "RS": ["seller_rs_1", "seller_rs_2"]
}

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]

# Event that pauses the producer while the pipeline is running
pipeline_running = threading.Event()

# Run silver and gold pipeline in background every 30 seconds
is_pipeline_running = False

def run_pipeline():
    global is_pipeline_running
    while True:
        time.sleep(30)
        print("\nPipeline starting - pausing orders...")
        is_pipeline_running = True

        subprocess.run([sys.executable, "../src/silver/silver_transformer.py"], check=True)
        subprocess.run([sys.executable, "../src/gold/gold_aggregator.py"], check=True)

        is_pipeline_running = False
        print("Pipeline complete - resuming orders!\n")
# Start pipeline thread in background
pipeline_thread = threading.Thread(target=run_pipeline, daemon=True)
pipeline_thread.start()

producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_string,
    eventhub_name=event_hub_name
)

print("Sending live orders to Event Hub...")

while True:
    if is_pipeline_running:
        print("Waiting for pipeline...")
        while is_pipeline_running:
            time.sleep(1)
        print("Resuming orders...")
        continue

    customer_state = random.choice(STATES)
    seller_state = random.choice(list(SELLERS.keys()))
    seller_id = random.choice(SELLERS[seller_state])
    category = random.choice(CATEGORIES)
    price = round(random.uniform(20.0, 500.0), 2)
    freight = round(random.uniform(5.0, 50.0), 2)
    installments = random.randint(1, 12)
    payment_type = random.choice(PAYMENT_TYPES)

    purchase_time = datetime.now(timezone.utc)
    estimated_days = random.randint(7, 20)
    actual_days = random.randint(5, 25)
    estimated_delivery = purchase_time + timedelta(days=estimated_days)
    actual_delivery = purchase_time + timedelta(days=actual_days)

    order = {
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "order_status": "delivered",
        "order_purchase_timestamp": purchase_time.isoformat(),
        "order_delivered_customer_date": actual_delivery.isoformat(),
        "order_delivered_carrier_date": (purchase_time + timedelta(days=2)).isoformat(),
        "order_estimated_delivery_date": estimated_delivery.isoformat(),
        "customer_state": customer_state,
        "seller_id": seller_id,
        "seller_state": seller_state,
        "product_category_name": category,
        "payment_value": price + freight,
        "payment_type": payment_type,
        "payment_installments": installments,
        "price": price,
        "freight_value": freight
    }

    event_json = json.dumps(order)
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(event_json))
    producer.send_batch(event_data_batch)

    print(f"Sent order: {order['order_id']} | {customer_state} | {category} | R$ {order['payment_value']}")
    time.sleep(2)