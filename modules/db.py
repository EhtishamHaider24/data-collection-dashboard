import os
import mysql.connector
from contextlib import contextmanager
from config.db_config import DB_CONFIG, REPORTING_DB_CONFIG
from modules.utils import setup_logger

logger = setup_logger("DB_LOGGER", os.path.join("logs", "db.log"))


@contextmanager
def get_connection(reporting=False):
    cfg = REPORTING_DB_CONFIG if reporting else DB_CONFIG
    try:
        conn = mysql.connector.connect(**cfg)
        yield conn
    except mysql.connector.Error as e:
        logger.error(f"MYSQL CONNECTION ERROR: {str(e).upper()}")
        raise
    finally:
        try:
            conn.close()
        except:
            pass


def execute_query(query, fetch=True, reporting=False):
    try:
        with get_connection(reporting=reporting) as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            if fetch:
                result = cursor.fetchall()
            else:
                conn.commit()
                result = None
            cursor.close()
        logger.info("QUERY EXECUTED SUCCESSFULLY".upper())
        return result
    except Exception as e:
        logger.error(f"QUERY EXECUTION ERROR: {str(e).upper()}")
        raise
