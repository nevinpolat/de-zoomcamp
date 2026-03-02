import duckdb

con = duckdb.connect("taxi_pipeline.duckdb")
table_name = "taxi_pipeline_dataset.taxi_data"

# Count proportion of trips paid with credit card
result = con.execute(f"""
    SELECT 
        100.0 * SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) / COUNT(*) AS credit_card_pct
    FROM {table_name}
""").fetchone()

print(f"Proportion of trips paid with credit card: {result[0]:.2f}%")