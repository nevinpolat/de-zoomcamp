import pandas as pd

# Load files
green_df = pd.read_parquet("data/green_tripdata_2025-11.parquet")
zone_df = pd.read_csv("data/taxi_zone_lookup.csv")

# Peek at the data
#print(green_df.head())
#print(green_df.columns)
print(zone_df.columns)
#zone_df.columns

