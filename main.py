import os
from modules.collector import run_all_general, run_region_loop
from modules.utils import setup_logger

logger = setup_logger("MAIN_LOGGER", os.path.join("logs", "main.log"))

if __name__ == "__main__":
    logger.info("STARTING COLLECTION REPORT JOB".upper())
    try:
        logger.info(
            "PROCESSING GENERAL STATS FOR ALL COLLECTED_BY TYPES".upper())
        run_all_general()
        logger.info("GENERAL STATS PROCESSING COMPLETED".upper())

        logger.info(
            "PROCESSING REGION-WISE STATS FOR ALL COLLECTED_BY TYPES".upper())
        run_region_loop()
        logger.info("REGION-WISE STATS PROCESSING COMPLETED".upper())

        logger.info("COLLECTION REPORT JOB COMPLETED SUCCESSFULLY".upper())
    except Exception as e:
        logger.error(f"COLLECTION REPORT JOB FAILED: {str(e).upper()}")
        raise
