from pathlib import Path
import os, logging, requests
from dotenv import load_dotenv

import pandas as pd
from datetime import date

from databricks import sql

load_dotenv()

log_dir = Path("../logs")
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,  # Minimum level to capture
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_dir / "pipeline.log"),  # Write pipeline log to a file for production
        logging.StreamHandler(),  # Stream log into console
    ],
)

# Set info on Transformation logger & Databricks logs into warning or higher
logger = logging.getLogger("API Ingestion")
logging.getLogger("databricks.sql").setLevel(logging.WARNING)

# Databricks Workspace
CATALOG = "fx_pipeline_dev"
SCHEMA = "bronze"
TABLE_NAME = "usd_idr_rates"

def extract_data(api_url):
    try:
        params = {
            "base": "USD",
            "quotes": "IDR",
            "from": "2026-01-01",
            "to": "2026-06-30"
        }

        logger.info(f"Starting data ingestion from Frankfurter: {api_url}")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status() # Raise for API Status

        df = pd.DataFrame(response.json())
        logger.info(f"Successfully Fetched {len(df)} records from Frankfurter.")  
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"API Request Failed: {e}")

    except Exception as e:
        logger.error(f"Unexpected Error: {e}")

def load_to_databricks(df):
    try:
        logger.info(f"Connect Databricks Server")
        with sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        ) as connection:
            logger.info(f"Loading data....")
            with connection.cursor() as cursor:
                # Insert records using parameterized queries for security & performance
                records = [tuple(row) for row in df.to_numpy()]

                # Dropping Table if exists
                cursor.execute(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{TABLE_NAME}")

                # Recreate Delta Table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{TABLE_NAME} (
                        date DATE,
                        base STRING,
                        quote STRING,
                        rate FLOAT
                    )
                """)

                insert_query = f"""
                            INSERT INTO {CATALOG}.{SCHEMA}.{TABLE_NAME} 
                            (date, base, quote, rate)
                            VALUES (?, ?, ?, ?)
                        """

                cursor.executemany(insert_query, records)
                logger.info(f"Successfully loaded {len(records)} rows into {CATALOG}.{SCHEMA}.{TABLE_NAME}.")

    except Exception as e:
        logger.error(f"Data load failed: {e}")

if __name__ == "__main__":
    api_data = extract_data("https://api.frankfurter.dev/v2/rates")
    if api_data is not None and not api_data.empty:
        load_to_databricks(api_data)
    else:
        logger.warning("No data to load into databricks")