import os, json, time, requests
from datetime import datetime, timezone
import logging
import os, glob
from sqlalchemy import URL, create_engine, text
from requests.exceptions import ConnectionError, HTTPError

logging.basicConfig(level=logging.INFO)
class FrankfurterExtractor:
    def __init__(self, URL, start_year, end_year):
        self.URL = URL
        self.start_year = start_year
        self.end_year = end_year

    def extract_data(self):
        os.makedirs("data_fx", exist_ok=True)
        logging.info("Data extraction started.")
        for year in range(self.start_year, self.end_year + 1):
            params = {"base": "usd", "quotes": "egp",
                        "from": f"{year}-01-01", "to": f"{year}-12-31"}
            logging.info(f"Fetching data for year: {year}")
            try:
                response = requests.get(self.URL, params=params)
                logging.info(f"Response status code for year {year}: {response.status_code}")
                response.raise_for_status()  # Raise an exception for HTTP errors
            except ConnectionError as con_err:
                logging.error(f"Connection error occurred for year {year}: {con_err}")
                continue  # Skip to the next iteration
            except HTTPError as http_err:
                logging.error(f"HTTP error occurred for year {year}: {http_err}")
                continue  # Skip to the next iteration

            if response.status_code == 200:
                records = response.json()
                logging.info(f"Records for year {year}: {len(records)}")

                if records:
                    extracted_at = datetime.now(timezone.utc).isoformat()
                    for row in records:
                        row["_ingested_at"] = extracted_at

                    with open(f"data_fx/{year}.json", "w") as f:
                        json.dump(records, f)

            time.sleep(7)  # Sleep for 7 seconds to avoid hitting the API rate limit
        logging.info(f"Completed fetching data from {self.start_year} to {self.end_year}")
    
    
    def load_bronze(self):
        engine = create_engine(f"postgresql+psycopg://postgres:{os.environ['POSTGRES_PASSWORD']}@db:5432/postgres")

        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS bronze.fx_rates_raw (file TEXT, payload JSONB)"))
            conn.execute(text("TRUNCATE bronze.fx_rates_raw"))  # avoids duplicates on reruns

            for path in sorted(glob.glob("data_fx/*.json")):  # one file per year
                with open(path) as f:
                    payload = f.read()
                conn.execute(text("INSERT INTO bronze.fx_rates_raw VALUES (:f, CAST(:p AS JSONB))"),
                                {"f": path, "p": payload})
                logging.info(f"Loaded {path} into bronze.fx_rates_raw")


URL = "https://api.frankfurter.dev/v2/rates"
Start_year = 1994
End_year = 2026
frankfurter_extractor = FrankfurterExtractor(URL, Start_year, End_year)
frankfurter_extractor.extract_data()
frankfurter_extractor.load_bronze()