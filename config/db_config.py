import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

REPORTING_DB_CONFIG = {
    "host": os.getenv("REPORTING_DB_HOST"),
    "port": int(os.getenv("REPORTING_DB_PORT")),
    "database": os.getenv("REPORTING_DB_NAME"),
    "user": os.getenv("REPORTING_DB_USER"),
    "password": os.getenv("REPORTING_DB_PASSWORD")
}
