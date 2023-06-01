import csv
from vnstock import get_index_series
import pandas as pd
from google.cloud import pubsub_v1

PATH_KEY = 'vnstock-381809-22dc568a0a39.json'
PROJECT_ID = 'vnstock-381809'
TOPIC_NAME = 'ticker_subscribe'

with open('subscribe.csv', 'r') as file:
    reader = csv.reader(file)
    ticker = [row[0] for row in reader]

data = []

for i in ticker:
    re = get_index_series(index_code=i, time_range='OneDay').drop(columns=['matchVolume', 'matchValue', 'totalMatchValue']).iloc[-1:]
    re.to_csv('msg.csv', header=None, index=None)
    with open('msg.csv', 'r') as file:
        reader = csv.reader(file)
        msg = [row for row in reader]
        data.append(msg[0])

publisher = pubsub_v1.PublisherClient.from_service_account_json(PATH_KEY)

for m in data:
    message_data = str(m).encode('utf-8')
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    future = publisher.publish(topic_path, data=message_data)
    print(f"Message published: {future.result()}")