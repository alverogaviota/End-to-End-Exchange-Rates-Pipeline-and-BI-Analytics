import os
import logging
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,  # Minimum level to capture
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),  # Write pipeline log to a file for production
        logging.StreamHandler(),  # Stream log into console
    ],
)

# Set info on Transformation logger & Databricks logs into warning or higher
logger = logging.getLogger("Transformation")
logging.getLogger("databricks.sql").setLevel(logging.WARNING)

# Databricks Workspace
CATALOG = "fx_pipeline_dev"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
TABLE_NAME = "usd_idr_rates"

def transform():
    try:
        logger.info(f"Connect Databricks Server")
        with sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}
                    AS SELECT
                        CAST(date AS DATE) AS date_rate,
                        base,
                        quote,
                        CAST(rate AS DECIMAL(18, 6)) as rate
                    FROM {CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}
                    WHERE date is NOT NULL AND rate > 0
                """)
        logger.info("Silver table refreshed")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")

if __name__ == "__main__":
    transform()