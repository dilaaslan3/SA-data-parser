import base64
from kafka import KafkaConsumer
import json
import csv
import os
from azure.storage.blob import BlobServiceClient

DATA_LIMIT = 300

def consume():
    print('Running Consumer..')
    raw_data = []  # list of incoming data from gagana
    topic_name = 'hello-mqtt-kafka'
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    for msg in consumer:
        raw_data.append(msg.value)
        if len(raw_data) == DATA_LIMIT:
            parse_data(raw_data)
    consumer.close()


def parse_data(raw_data):
    parsed_data = []  # array of dict


    for raw_line in raw_data:
        # string to json
        json_line_data = json.loads(raw_line)
        # getting payload from json
        print(json_line_data['payload'])
        # sample_raw_line = '{"schema":{"type":"bytes","optional":false},"payload":"eyJleGFuZyI6MCwic2V4IjoxLCJ0aGFsIjo2LCJjaG9sIjoyMzMsInNsb3BlIjozLCJjcCI6MSwidHJlc3RicHMiOjE0NSwib2xkcGVhayI6Mi4zLCJ0aGFsYWNoIjoxNTAsImZicyI6MSwicHJlZGljdGlvbiI6MCwiYWdlIjo2MywiY2EiOjAsInJlc3RlY2ciOjJ9"}'

        # decoding base64 payload
        decoded_data = base64.b64decode(json_line_data['payload'])
        print(decoded_data)
        # "payload":"eyJleGFuZyI6MCwic2V4IjoxLCJ0aGFsIjo2LCJjaG9sIjoyMzMsInNsb3BlIjozLCJjcCI6MSwidHJlc3RicHMiOjE0NSwib2xkcGVhayI6Mi4zLCJ0aGFsYWNoIjoxNTAsImZicyI6MSwicHJlZGljdGlvbiI6MCwiYWdlIjo2MywiY2EiOjAsInJlc3RlY2ciOjJ9"


        # decoded payload to json
        parsed_line = json.loads(decoded_data)
        print(parsed_line)
        parsed_data.append(parsed_line)
        # {"exang":0,"sex":1,"thal":6,"chol":233,"slope":3,"cp":1,"trestbps":145,"oldpeak":2.3,"thalach":150,"fbs":1,"prediction":0,"age":63,"ca":0,"restecg":2}

    dict_to_csv_file(parsed_data)


def dict_to_csv_file(parsed_data):

    csv_columns = ['exang', 'sex', 'thal', 'chol', 'slope', 'cp', 'trestbps', 'oldpeak', 'thalach', 'fbs', 'prediction',
                   'age', 'ca', 'restecg']
    csv_file = "output.csv"

    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for parsed_line in parsed_data:
                writer.writerow(parsed_line)

    except Exception as exception:
        print("exception while dict to csv")
        print(str(exception))

    upload_to_blob_storage()

def upload_to_blob_storage():
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

consume()
