import schedule
import subprocess
import time
import logging
import yaml
from pathlib import Path
from datetime import datetime

LOCK_FILE = Path("pipeline.lock")
LOG_FILE = Path("logs/scheduler_activity.log")
LOG_FILE.parent.mkdir(exist_ok=True)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Load config
# -----------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

RUN_TIME = config["scheduler"]["run_time"]

# -----------------------------
# Pipeline Execution
# -----------------------------
def run_pipeline():
    if LOCK_FILE.exists():
        logging.warning("Pipeline already running. Skipping execution.")
        return

    try:
        LOCK_FILE.touch()
        logging.info("🚀 Scheduled pipeline execution started")

        result = subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logging.info("✅ Pipeline completed successfully")
            logging.info(result.stdout)

            # Run cleanup only after success
            subprocess.run(
                ["python", "scripts/cleanup_old_data.py"],
                capture_output=True,
                text=True
            )
            logging.info("🧹 Cleanup completed")

        else:
            logging.error("❌ Pipeline failed")
            logging.error(result.stderr)

    except Exception as e:
        logging.error(f"Scheduler execution error: {e}")

    finally:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
        logging.info("Pipeline lock released")


# -----------------------------
# Scheduler Setup
# -----------------------------
schedule.every().day.at(RUN_TIME).do(run_pipeline)

logging.info(f"📅 Scheduler started — daily at {RUN_TIME}")

while True:
    try:
        schedule.run_pending()
        time.sleep(60)
    except Exception as e:
        logging.error(f"Scheduler runtime error: {e}")
        time.sleep(60)