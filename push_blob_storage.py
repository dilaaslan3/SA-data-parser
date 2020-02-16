from datetime import datetime
import os, uuid
from azure.storage.blob import BlobServiceClient

try:
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    local_file_name = "output.csv"
    output_file_name = "processed_output.csv"
    container_name = "cleveland1"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_file_name)

    print(f"Uploading '{local_file_name}' to blob storage with name '{output_file_name}'....")
    with open(f"{local_file_name}", "rb") as data:
        blob_client.upload_blob(data)
    print("File successfully uploaded to Azure Blob Storage.")

except Exception as ex:
    print('Exception:')
    print(ex)
