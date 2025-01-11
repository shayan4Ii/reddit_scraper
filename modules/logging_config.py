# logging_config.py
import logging
from logging.handlers import RotatingFileHandler

LOG_FILE = "reddit_scraper.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5),  # 5 MB per file, keep 5 backups
        logging.StreamHandler()
    ]
)

