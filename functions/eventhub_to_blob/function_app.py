import azure.functions as func
import logging
import json
from datetime import datetime

app = func.FunctionApp()


@app.function_name(name="eventhub_to_blob")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="orders",
    connection="EVENTHUB_CONNECTION"
)
def eventhub_to_blob(event: func.EventHubEvent):
    logging.info("Event received!")

    body = event.get_body().decode("utf-8")
    logging.info(f"Event body: {body}")
