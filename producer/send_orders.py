import os # To read env
import json #To transform python dict to json string 
from datetime import datetime, timezone
from dotenv import load_dotenv #This loads the .env file so the variables can be accesed by the script
from azure.eventhub import EventData, EventHubProducerClient


load_dotenv()
connection_string = os.getenv("EVENT_HUB_CONNECTION_STRING")
event_hub_name = os.getenv("EVENT_HUB_NAME")
order = {
    "order_id": "order_123",
    "event_time": datetime.now(timezone.utc).isoformat(),
    "customer_id": "cust_456",
    "country": "GR",
    "device": "mobile",
    "payment_method": "card",
    "items": [
        {"sku": "sku_1", "qty": 2, "price": 10.0},
        {"sku": "sku_2", "qty": 1, "price": 25.0}
    ],
    "total_amount": 45.0,
    "currency": "EUR",
    "status": "created"
}

event_json = json.dumps(order)
producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_string,
    eventhub_name=event_hub_name
)
event_data_batch = producer.create_batch()
event_data_batch.add(EventData(event_json))
producer.send_batch(event_data_batch)
producer.close()
print("Event sent successfully to Event Hub")