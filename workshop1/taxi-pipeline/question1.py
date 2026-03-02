import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")

table_name = "taxi_pipeline_dataset.taxi_data"

result = con.execute(f"""
    SELECT 
        MIN(trip_pickup_date_time) AS start_date,
        MAX(trip_pickup_date_time) AS end_date
    FROM {table_name}
""").fetchall()

start_date, end_date = result[0]
print(f"Start date: {start_date}")
print(f"End date: {end_date}")