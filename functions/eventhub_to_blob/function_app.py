import azure.functions as func
import logging
import json
from datetime import datetime
import uuid
#We improt os to access environment variables, and we import BlobServiceClient to interact with Azure Blob Storage.
import os
from azure.storage.blob import BlobServiceClient 
#Import the BlobServiceClient class from the azure.storage.blob module to interact with Azure Blob Storage

app = func.FunctionApp()
@app.function_name(name="eventhub_to_blob")#Name of the function
#Trigger the function when a message is received in the
#event hub named orders, using the connection string specified 
#in the application settings under the key EVENTHUB_CONNECTION
@app.event_hub_message_trigger(
    arg_name="event",#Name of the argument that will receive the event data
    event_hub_name="orders",#Connected to the event hub named orders
    connection="EVENTHUB_CONNECTION"
)

#This function will be triggered when a message is received
#in the event hub named orders. The event data will be passed
#to the function as an argument named event.
#The function will log the event body to the console.
def eventhub_to_blob(event: func.EventHubEvent):
    logging.info("Event received!")
    body = event.get_body().decode("utf-8")#It returns bytes, so we need to decode it to a string!!!
    try:
        #The goal is to check if the body is a valid JSON
        data=json.loads(body)#We create a new variable data that it is a dictionary that contains the parsed JSON data
        #Log the order_id if it exists in the JSON data
        daily_date=datetime.utcnow()
        year=daily_date.year
        month=str(daily_date.month).zfill(2)
        day=str(daily_date.day).zfill(2)
        unique_id=str(uuid.uuid4())#Generate a unique identifier for the blob name
        blob_name=f"orders/{year}/{month}/{day}/{unique_id}.json"
        #We take the connection string for Azure Blob Storage from the environment variable AzureWebJobsStorage,
        #which is typically used in Azure Functions to store data and manage state.
        connection_string=os.environ.get("AzureWebJobsStorage")
        if connection_string is None:
            logging.info("Missing AzureWebJobsStorage")
            return
        logging.info("Found AzureWebJobsStorage")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("bronze")
        container_client.upload_blob(
            name=blob_name,
            data=body,
            overwrite=False #So that one event does not step on another
        )
        logging.info(f"Uploaded blob: {blob_name}")
        logging.info(f"Parsed order_id:{data.get('order_id')}")
        
    except Exception as e:
        logging.error(f"JSON parse faild: {e}")
        return

