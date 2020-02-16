import base64
from kafka import KafkaConsumer
import json
import csv

def consumerSa():
    print('Running Consumer..')
    raw_data = [] #list of incoming data from gagana
    topic_name = 'hello-mqtt-kafka'
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    msg = 0 # incoming data = x
    for msg in consumer:
        raw_data.append(msg)
        msg = msg +1
        if len(raw_data) == 300:
            parse_data(raw_data)


#datada sadece payload kısmını alıcaz
#payloadu json a dönüştürecez
#jsonu csv ye dönüşür
#csv nin size kontrolünü yap
#istenilen boyuta geldiği zaman data lake e gönder

def parse_data(raw_data):


    parsed_data =  [] #array of csv data


    x = '{"schema":{"type":"bytes","optional":false},"payload":"eyJleGFuZyI6MCwic2V4IjoxLCJ0aGFsIjo2LCJjaG9sIjoyMzMsInNsb3BlIjozLCJjcCI6MSwidHJlc3RicHMiOjE0NSwib2xkcGVhayI6Mi4zLCJ0aGFsYWNoIjoxNTAsImZicyI6MSwicHJlZGljdGlvbiI6MCwiYWdlIjo2MywiY2EiOjAsInJlc3RlY2ciOjJ9"}'

    for raw_line in raw_data:

        #string to json
        json_line_data = json.loads(raw_line)
        #getting payload from json
        print(json_line_data['payload'])

        #decoding base64 payload
        decoded_data = base64.b64decode(json_line_data['payload'])
        print(decoded_data)

        #decoded payload to json
        print(json.loads(decoded_data))

    #json to csv
    #file(parsed_data)
    diladict = {'exang': 0, 'sex': 1, 'thal': 6, 'chol': 233, 'slope': 3, 'cp': 1, 'trestbps': 145, 'oldpeak': 2.3, 'thalach': 150, 'fbs': 1, 'prediction': 0, 'age': 63, 'ca': 0, 'restecg': 2}
    a = diladict['sex']
    print(a)

def file(parsed_data):
    f = open("output.csv", "a")
    f.write("Now the file has more content!")
    f.close()

parse_data([])