import pandas as pd
from sqlalchemy import create_engine
import os

DB_USER = 'root'
DB_PASSWORD = 'root'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ny_taxi'


# Create SQLAlchemy engine
engine = create_engine(
    f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

# Load Taxi Zone Lookup Data
zone_df = pd.read_csv('data/taxi_zone_lookup.csv')
zone_df.to_sql('taxi_zone_lookup', engine, if_exists='replace', index=False)
print("Loaded taxi_zone.csv into PostgreSQL.")

# Load Green Taxi Trips Data (Parquet)
green_df = pd.read_parquet('data/green_tripdata_2025-11.parquet')

# Optional: load in chunks to avoid memory issues
chunk_size = 100_000

for i in range(0, len(green_df), chunk_size):
    chunk = green_df.iloc[i:i + chunk_size]
    chunk.to_sql(
        'green_tripdata',
        engine,
        if_exists='append',
        index=False
    )
    print(f"Appended rows {i} to {i + len(chunk)} into green_tripdata.")

print("All data loaded successfully.")
