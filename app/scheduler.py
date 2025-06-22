import schedule
import time
import logging
import os
from dotenv import load_dotenv
from main import main

load_dotenv()

logger = logging.getLogger(__name__)

def run_daily_extraction():
    """Wrapper function for scheduled execution"""
    logger.info("Scheduled extraction starting...")
    try:
        main()
        logger.info("Scheduled extraction completed successfully")
    except Exception as e:
        logger.error(f"Scheduled extraction failed: {e}")

def start_scheduler():
    """Start the daily scheduler"""
    daily_run_time = os.getenv("DAILY_RUN_TIME", "17:00")  # Default to 5 PM
    
    schedule.every().day.at(daily_run_time).do(run_daily_extraction)
    logger.info(f"Scheduler started. Daily extraction scheduled for {daily_run_time}")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    start_scheduler()