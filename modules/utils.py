import logging
from datetime import datetime
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_logger(name, log_file, level=logging.INFO):
    """Setting up logging."""

    abs_log_path = os.path.join(PROJECT_ROOT, log_file)

    os.makedirs(os.path.dirname(abs_log_path), exist_ok=True)

    handler = logging.FileHandler(abs_log_path)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default
