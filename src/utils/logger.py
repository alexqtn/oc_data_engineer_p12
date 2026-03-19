# ============================================================
# logger.py — Centralized logging for Sport Data Solution
# Entry point: get_logger(__name__) returns a configured logger
# Output: console + daily rotating file in logs/
# ============================================================

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


# Log folder at project root (2 levels up from src/utils/)
ROOT_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "sport_data.log"

# Format: timestamp | level | module | message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that writes to console + daily rotating file.
    Call with: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if get_logger() called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Console handler — live output during development
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # File handler — new file every midnight, keep 30 days
    file_handler = TimedRotatingFileHandler(
        filename=LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.suffix = "%Y-%m-%d"

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger