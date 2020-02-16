import base64
from kafka import KafkaConsumer
import json
import csv


def consumerSa():
    print('Running Consumer..')
    raw_data = []  # list of incoming data from gagana
    topic_name = 'hello-mqtt-kafka'
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    msg = 0  # incoming data = x
    for msg in consumer:
        raw_data.append(msg)
        msg = msg + 1
        if len(raw_data) == 300:
            parse_data(raw_data)


# datada sadece payload kısmını alıcaz
# payloadu json a dönüştürecez
# jsonu csv ye dönüşür
# csv nin size kontrolünü yap
# istenilen boyuta geldiği zaman data lake e gönder


def parse_data(raw_data):
    parsed_data = []  # array of dict

    x = '{"schema":{"type":"bytes","optional":false},"payload":"eyJleGFuZyI6MCwic2V4IjoxLCJ0aGFsIjo2LCJjaG9sIjoyMzMsInNsb3BlIjozLCJjcCI6MSwidHJlc3RicHMiOjE0NSwib2xkcGVhayI6Mi4zLCJ0aGFsYWNoIjoxNTAsImZicyI6MSwicHJlZGljdGlvbiI6MCwiYWdlIjo2MywiY2EiOjAsInJlc3RlY2ciOjJ9"}'

    for raw_line in raw_data:
        # string to json
        json_line_data = json.loads(raw_line)
        # getting payload from json
        print(json_line_data['payload'])

        # decoding base64 payload
        decoded_data = base64.b64decode(json_line_data['payload'])
        print(decoded_data)

        # decoded payload to json
        parsed_line = json.loads(decoded_data)
        print(parsed_line)
        parsed_data.append(parsed_line)

    dict_to_csv_file(parsed_data)


def dict_to_csv_file(parsed_data):
    # parsed_line -> dila_dict

    #TODO: delete diladict on the final version
    diladict = {'exang': 0, 'sex': 1, 'thal': 6, 'chol': 233, 'slope': 3, 'cp': 1, 'trestbps': 145, 'oldpeak': 2.3,
                'thalach': 150, 'fbs': 1, 'prediction': 0, 'age': 63, 'ca': 0, 'restecg': 2}
    parsed_data.append(diladict)


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



parse_data([])
