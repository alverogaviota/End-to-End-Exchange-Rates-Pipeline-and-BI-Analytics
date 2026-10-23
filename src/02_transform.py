import os, logging
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

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
                logger.info(f"Transforming Data...")
                cursor.execute(f"""
                    CREATE OR REPLACE TABLE {CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}
                    AS SELECT
                        CAST(date AS DATE) AS rate_date,
                        base,
                        quote,
                        CAST(rate AS DECIMAL(10, 2)) as rate
                    FROM {CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}
                    WHERE date is NOT NULL AND rate > 0
                """)
        logger.info(f"Data Successfully Transformed in {CATALOG}.{SILVER_SCHEMA}.{TABLE_NAME}")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")

if __name__ == "__main__":
    try:
        transform()
    except Exception as e:
        logger.error(f"Transformation failed: {e}")