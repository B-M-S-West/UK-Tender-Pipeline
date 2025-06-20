import logging
import sys
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from api_client import TenderAPIClient
from data_processor import TenderDataProcessor
from airtable_manager import AirtableManager

load_dotenv()

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f"tender_extract_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main execution function for daily tender extraction"""
    logger.info("Starting daily tender extraction process")
    
    try:
        # Initialize components
        api_client = TenderAPIClient()
        processor = TenderDataProcessor()
        airtable_manager = AirtableManager()
        
        # Fetch data for both stages
        all_data = api_client.fetch_all_stages()
        
        total_processed = 0
        
        # Process tender data
        if all_data["tender"]:
            tender_records = processor.process_releases(all_data["tender"], "tender")
            if tender_records:
                results = airtable_manager.batch_upsert(tender_records, "tender")
                logger.info(f"Tender processing: {results['success']} success, {results['failed']} failed")
                total_processed += len(tender_records)
        
        # Process pipeline data
        if all_data["pipeline"]:
            pipeline_records = processor.process_releases(all_data["pipeline"], "pipeline")
            if pipeline_records:
                results = airtable_manager.batch_upsert(pipeline_records, "pipeline")
                logger.info(f"Pipeline processing: {results['success']} success, {results['failed']} failed")
                total_processed += len(pipeline_records)
        
        logger.info(f"Daily extraction completed. Total records processed: {total_processed}")
        
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()