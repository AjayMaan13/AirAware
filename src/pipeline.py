# src/pipeline.py

import logging
import time
import schedule
from datetime import datetime
import sys
import os

# Import our ETL modules
import extract
import transform
import load

# Import configuration
from config import (
    LOG_FILE,
    LOG_LEVEL,
    PIPELINE_INTERVAL_HOURS
)

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline():
    """
    Run the complete ETL pipeline: extract, transform, load.
    """
    start_time = datetime.now()
    logger.info(f"Starting air quality ETL pipeline at {start_time}")
    
    # Step 1: Extract data
    logger.info("Starting data extraction step")
    extract.main()
    logger.info("Data extraction completed")
    
    # Step 2: Transform data
    logger.info("Starting data transformation step")
    transform.main()
    logger.info("Data transformation completed")
    
    # Step 3: Load data
    logger.info("Starting data loading step")
    load.main()
    logger.info("Data loading completed")
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"ETL pipeline completed at {end_time}. Duration: {duration}")
    
    return True

def schedule_pipeline():
    """
    Schedule the pipeline to run at regular intervals.
    """
    logger.info(f"Scheduling pipeline to run every {PIPELINE_INTERVAL_HOURS} hours")
    
    # Schedule the job
    schedule.every(PIPELINE_INTERVAL_HOURS).hours.do(run_pipeline)
    
    # Print next run time
    next_run = schedule.next_run()
    logger.info(f"Next pipeline run scheduled for: {next_run}")
    
    # Run once immediately
    logger.info("Running pipeline immediately for initial data population")
    run_pipeline()
    
    # Keep the script running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Pipeline scheduler stopped by user")
    except Exception as e:
        logger.error(f"Pipeline scheduler stopped due to error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--run-once":
        # Run the pipeline once without scheduling
        run_pipeline()
    else:
        # Schedule the pipeline to run repeatedly
        schedule_pipeline()