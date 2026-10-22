import os
import logging
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,  # Minimum level to capture
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("../logs/pipeline.log"),  # Write pipeline log to a file for production
        logging.StreamHandler(),  # Stream log into console
    ],
)

# Set info on Transformation logger & Databricks logs into warning or higher
logger = logging.getLogger("Aggregation")
logging.getLogger("databricks.sql").setLevel(logging.WARNING)

# Databricks Workspace
CATALOG = "fx_pipeline_dev"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

rates_daily = f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.rates_daily
WITH with_prev AS (
  SELECT
    rate_date,
    base,
    quote,
    rate,
    LAG(rate) OVER (PARTITION BY base, quote ORDER BY rate_date) AS prev_rate
  FROM {CATALOG}.{SILVER_SCHEMA}.usd_idr_rates
)
SELECT
  rate_date,
  base,
  quote,
  rate,
  COALESCE(rate - prev_rate, 0) AS daily_change,
  COALESCE(ROUND((rate - prev_rate) / prev_rate * 100, 4), 0) AS daily_change_pct
FROM with_prev;
"""

rates_monthly = f"""
CREATE OR REPLACE TABLE {CATALOG}.{GOLD_SCHEMA}.rates_monthly
WITH monthly AS (
  SELECT
    date_format(rate_date, 'yyyy-MM') AS month,
    base,
    quote,
    AVG(rate) AS avg_rate,
    MAX(rate) AS high_rate,
    MIN(rate) AS low_rate,
    max_by(rate, rate_date) AS closing_rate,
    COUNT(*) AS days_with_data
  FROM {CATALOG}.{SILVER_SCHEMA}.usd_idr_rates
  GROUP BY date_format(rate_date, 'yyyy-MM'), base, quote
),
with_prev AS (
  SELECT
    *,
    LAG(closing_rate) OVER (PARTITION BY base, quote ORDER BY month) AS prev_closing_rate
  FROM monthly
)
SELECT
  month,
  base,
  quote,
  avg_rate,
  high_rate,
  low_rate,
  closing_rate,
  days_with_data,
  COALESCE(closing_rate - prev_closing_rate, 0) AS mtm_change,
  COALESCE(ROUND((closing_rate - prev_closing_rate) / prev_closing_rate * 100, 4), 0) AS mtm_change_pct
FROM with_prev;
"""

def aggregate_metrics():
    try:
        logger.info(f"Connect Databricks Server")
        with sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        ) as connection:
            with connection.cursor() as cursor:
                # Daily Rates Table
                cursor.execute(rates_daily)
                # Monthly Rates Table
                cursor.execute(rates_monthly)
        logger.info("Gold metrics refreshed")

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")

if __name__ == "__main__":
    try:
        aggregate_metrics()
    except Exception as e:
        logger.error(f"Aggregation failed: {e}")