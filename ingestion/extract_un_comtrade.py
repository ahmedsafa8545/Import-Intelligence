import os, json, time, requests
from datetime import datetime, timezone
import logging
import os, glob
from sqlalchemy import create_engine, text
from requests.exceptions import ConnectionError, HTTPError

logging.basicConfig(level=logging.INFO)

class UNComtradeExtractor:
    def __init__(self, URL, start_year, end_year, flow:list):
        self.URL = URL
        self.start_year = start_year
        self.end_year = end_year
        self.flow = flow
    
    def extract_data(self):
        headers = {"Ocp-Apim-Subscription-Key": os.environ["API_KEY"]}
        for flow in self.flow:
            os.makedirs(f"data_{flow}", exist_ok=True)

            for year in range(self.start_year, self.end_year + 1):
                params = {
                            "reporterCode": 818,
                            "flowCode": flow,
                            "period": str(year),
                            "cmdCode": "AG6",
                            "maxRecords": 100000,
                            "includeDesc": "true"
                        }
                logging.info(f"Fetching {flow} - {year}")
                try:
                    response = requests.get(self.URL, headers=headers, params=params)
                    logging.info(f"Response status code: {response.status_code}")
                    response.raise_for_status()  # Raise an exception for HTTP errors
                except ConnectionError as con_err:
                    logging.error(f"Connection error occurred: {con_err}")
                    continue  # Skip to the next iteration
                except HTTPError as http_err:
                    logging.error(f"HTTP error occurred: {http_err}")
                    continue  # Skip to the next iteration

                if response.status_code == 200:
                    records = response.json().get("data", [])
                    logging.info(f"Fetched {len(records)} records for {flow} - {year}")

                    if records:
                        extracted_at = datetime.now(timezone.utc).isoformat()
                        for row in records:
                            row["_ingested_at"] = extracted_at
                            row["_flow"] = flow
                    logging.info(f"metadata added for {flow} - {year}")
                    with open(f"data_{flow}/{year}.json", "w") as f:
                        json.dump(records, f)
            
            logging.info(f"Completed fetching data for {flow} from {self.start_year} to {self.end_year}")

        time.sleep(3)  # Sleep for 3 seconds to avoid hitting the API rate limit
        

    def load_to_bronze(self):
        engine = create_engine(f"postgresql+psycopg://postgres:{os.environ['POSTGRES_PASSWORD']}@db:5432/postgres")

        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
            logging.info("Created schema bronze if not exists")
            conn.execute(text("CREATE TABLE IF NOT EXISTS bronze.comtrade_raw (file TEXT, payload JSONB)"))
            logging.info("Created table bronze.comtrade_raw if not exists")
            conn.execute(text("TRUNCATE bronze.comtrade_raw"))  # avoids duplicates on reruns
            logging.info("Loading data into bronze.comtrade_raw")
            
            for path in sorted(glob.glob("data_*/*.json")):  # reads both data_M and data_X
                with open(path) as f:
                    payload = f.read()
                conn.execute(text("INSERT INTO bronze.comtrade_raw VALUES (:f, CAST(:p AS JSONB))"),
                                {"f": path, "p": payload})
                logging.info(f"Loaded {path}")

flow = ["M", "X"]
url = "https://comtradeapi.un.org/data/v1/get/C/A/HS?"
headers = {"Ocp-Apim-Subscription-Key": os.environ["API_KEY"]}
start_year = 1994
end_year = 2026
uncomtrade_extractor = UNComtradeExtractor(url, start_year, end_year, flow)
uncomtrade_extractor.extract_data()
uncomtrade_extractor.load_to_bronze()