import dataclasses
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import pandas as pd
from kafka import KafkaProducer
from models_green import GreenRide, ride_from_row

# 1. Setup URL and Columns for Green Taxi
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 
    'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount'
]

# 2. Download ENTIRE dataset
print("Downloading data...")
df = pd.read_parquet(url, columns=columns)

def ride_serializer(ride):
    return json.dumps(dataclasses.asdict(ride)).encode('utf-8')

server = 'localhost:9092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

topic_name = 'green-trips'

# 3. Timing the production
t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)

# 4. Flush is critical for accurate timing
producer.flush()
t1 = time.time()

print(f'Sent {len(df)} rows')
print(f'took {(t1 - t0):.2f} seconds')