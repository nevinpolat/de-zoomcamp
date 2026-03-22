import json
from dataclasses import dataclass

@dataclass
class GreenRide:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: float
    trip_distance: float
    tip_amount: float
    total_amount: float

import numpy as np

def ride_from_row(row):
    # Check for None or NaN in passenger_count and other numeric fields
    passenger_count = row['passenger_count']
    if passenger_count is None or (isinstance(passenger_count, float) and np.isnan(passenger_count)):
        passenger_count = 0.0  # Or set to None if you prefer

    trip_distance = row['trip_distance']
    if trip_distance is None or (isinstance(trip_distance, float) and np.isnan(trip_distance)):
        trip_distance = 0.0

    tip_amount = row['tip_amount']
    if tip_amount is None or (isinstance(tip_amount, float) and np.isnan(tip_amount)):
        tip_amount = 0.0

    total_amount = row['total_amount']
    if total_amount is None or (isinstance(total_amount, float) and np.isnan(total_amount)):
        total_amount = 0.0

    return GreenRide(
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=passenger_count,
        trip_distance=trip_distance,
        tip_amount=tip_amount,
        total_amount=total_amount,
    )

def ride_deserializer(v):
    if v is None:
        return None
    try:
        # Decode bytes to string and then load JSON, then convert to GreenRide instance
        ride_dict = json.loads(v.decode('utf-8'))
        return GreenRide(**ride_dict)  # Convert dictionary to GreenRide instance
    except Exception as e:
        print(f"Error deserializing: {e}")
        return None