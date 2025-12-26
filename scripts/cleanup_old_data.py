import os
import time
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import logging

# -----------------------------
# Load config
# -----------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = config["scheduler"].get("retention_days", 7)
CUTOFF_DATE = datetime.now() - timedelta(days=RETENTION_DAYS)

TARGET_DIRS = [
    Path("data/raw"),
    Path("data/staging"),
    Path("logs")
]

# -----------------------------
# Logging
# -----------------------------
LOG_FILE = Path("logs/scheduler_activity.log")
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Cleanup Logic
# -----------------------------
def should_preserve(file_path: Path) -> bool:
    name = file_path.name.lower()

    if "metadata" in name or "summary" in name or "report" in name:
        return True

    # Protect today's files
    if datetime.fromtimestamp(file_path.stat().st_mtime).date() == datetime.today().date():
        return True

    return False


def cleanup_old_files():
    deleted_files = 0

    for directory in TARGET_DIRS:
        if not directory.exists():
            continue

        for file in directory.glob("*"):
            if not file.is_file():
                continue

            if should_preserve(file):
                continue

            file_age = datetime.fromtimestamp(file.stat().st_mtime)

            if file_age < CUTOFF_DATE:
                file.unlink()
                deleted_files += 1
                logging.info(f"Deleted old file: {file}")

    logging.info(f"Cleanup completed. Files removed: {deleted_files}")


if __name__ == "__main__":
    logging.info("Starting cleanup task")
    cleanup_old_files()
    logging.info("Cleanup task finished")