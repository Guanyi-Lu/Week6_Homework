import json
import csv
import time
from time import time
import pandas as pd
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

t0 = time()

topic_name = 'green-trips'
csv_file = 'green_tripdata_2019-10.csv'
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']

# Read CSV file using pandas
df = pd.read_csv(csv_file, usecols=columns)

# Replace NaN values with 0 for all columns
df.fillna(0, inplace=True)

# Convert datetime strings to timestamps (in milliseconds)
df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime']).astype('int64') // 10**6
df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime']).astype('int64') // 10**6

# Send data to Kafka
for index, row in df.iterrows():
    # Convert row to dictionary and ensure it is ready for serialization
    filtered_data = row.to_dict()

    # Send data to Kafka topic
    producer.send(topic_name, value=filtered_data)
    print(f"Sent: {filtered_data}")

producer.flush()
t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
