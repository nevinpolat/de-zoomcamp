import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models_green import ride_deserializer  # Ensure this points to the correct deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',  # Read from the earliest message
    group_id='green-trips-consumer',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name}...")

count = 0
long_trip_count = 0  # Counter for trips with distance greater than 5.0 km

# Process all messages
for message in consumer:
    ride = message.value
    if ride is not None:
        # Check if the trip distance is greater than 5.0
        if ride.trip_distance > 5.0:
            long_trip_count += 1
        
        count += 1
    
    # Debugging: Print the count of long trips every 100th ride
    if count % 6177 == 0:
        print(f"Processed {count} messages... Current trip_distance > 5.0 km count: {long_trip_count}")

# Final count after all messages are processed
print(f"\nTotal number of trips with distance > 5.0 km: {long_trip_count}")

consumer.close()  # Explicitly close the consumer after processing