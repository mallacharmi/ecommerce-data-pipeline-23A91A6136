import pandas as pd
import time
import json
import logging
from pathlib import Path
from datetime import datetime
import yaml
import os

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


# -----------------------------
# Paths
# -----------------------------
RAW_DATA_DIR = Path("data/raw")
STAGING_DATA_DIR = Path("data/staging")
LOG_DIR = Path("logs")
STAGING_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Logging Setup
# -----------------------------
log_file = LOG_DIR / f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# -----------------------------
# Load Config
# -----------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)
    
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# -----------------------------
# Validation Function
# -----------------------------
def validate_staging_load(engine, table_name, csv_rows):
    result = engine.execute(
        text(f"SELECT COUNT(*) FROM staging.{table_name}")
    ).scalar()
    return result == csv_rows, result


# -----------------------------
# Main Ingestion Logic
# -----------------------------
def ingest_to_staging():
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    engine = create_engine(DB_URL)

    tables = {
        "customers": "customers.csv",
        "products": "products.csv",
        "transactions": "transactions.csv",
        "transaction_items": "transaction_items.csv"
    }

    try:
        with engine.begin() as connection:  # BEGIN TRANSACTION
            logging.info("Transaction started")

            for table, file_name in tables.items():
                file_path = RAW_DATA_DIR / file_name

                if not file_path.exists():
                    raise FileNotFoundError(f"Missing file: {file_name}")

                df = pd.read_csv(file_path)

                logging.info(f"Truncating staging.{table}")
                connection.execute(text(f"TRUNCATE staging.{table}"))

                logging.info(f"Loading {len(df)} rows into staging.{table}")
                df.to_sql(
                    table,
                    con=connection,
                    schema="staging",
                    if_exists="append",
                    index=False,
                    method="multi"
                )

                valid, db_count = validate_staging_load(
                    connection, table, len(df)
                )

                if not valid:
                    raise ValueError(
                        f"Row count mismatch for {table}: CSV={len(df)}, DB={db_count}"
                    )

                summary["tables_loaded"][f"staging.{table}"] = {
                    "rows_loaded": len(df),
                    "status": "success",
                    "error_message": None
                }

            logging.info("All tables loaded successfully")

    except (FileNotFoundError, SQLAlchemyError, ValueError) as e:
        logging.error(str(e))
        summary["tables_loaded"]["error"] = {
            "status": "failed",
            "error_message": str(e)
        }
        raise

    finally:
        summary["total_execution_time_seconds"] = round(
            time.time() - start_time, 2
        )

        with open(STAGING_DATA_DIR / "ingestion_summary.json", "w") as f:
            json.dump(summary, f, indent=4)

        logging.info("Ingestion summary written")

    print("✅ Data ingestion into staging completed successfully")


# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    ingest_to_staging()