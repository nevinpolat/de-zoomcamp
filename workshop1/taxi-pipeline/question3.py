import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")
table_name = "taxi_pipeline_dataset.taxi_data"

# Sum the tips
result = con.execute(f"""
    SELECT SUM(tip_amt) AS total_tips
    FROM {table_name}
""").fetchone()

total_tips = result[0]
print(f"Total tips: ${total_tips:,.2f}")