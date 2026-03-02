import dlt
import requests
from typing import Any

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.resource(name="taxi_data", write_disposition="replace")
def get_taxi_data() -> Any:
    page = 0
    # Find the first non-empty page
    while True:
        response = requests.get(f"{BASE_URL}?page={page}")
        response.raise_for_status()
        data = response.json()
        if data:
            break
        page += 1

    print(f"Starting from page {page}")

    # Yield all pages
    while True:
        response = requests.get(f"{BASE_URL}?page={page}")
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        for record in data:
            yield record
        page += 1

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    progress="log",
)

if __name__ == "__main__":
    load_info = pipeline.run(get_taxi_data())
    print(load_info)