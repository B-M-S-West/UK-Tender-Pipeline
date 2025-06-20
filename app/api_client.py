import requests
import logging
import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class TenderAPIClient:
    def __init__(self):
        self.base_url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
        self.timeout = 30
        self.batch_size = 100
    
    def get_date_range(self):
        """Get date range for the last 24 hours"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        return {
            "start": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "end": end_time.strftime("%Y-%m-%dT%H:%M:%S")
        }
    
    def fetch_tenders(self, stage: str = "tender", limit: int = None) -> Optional[Dict]:
        """Fetch tenders from the API for the last 24 hours"""
        if limit is None:
            limit = self.batch_size
            
        date_range = self.get_date_range()
        
        params = {
            "updatedFrom": date_range["start"],
            "updatedTo": date_range["end"],
            "stages": stage,
            "limit": limit
        }
        
        try:
            logger.info(f"Fetching {stage} data from {date_range['start']} to {date_range['end']}")
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched {len(data.get('releases', []))} releases")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f'Error fetching {stage} data from API: {e}')
            return None
    
    def fetch_all_stages(self) -> Dict[str, Optional[Dict]]:
        """Fetch data for both tender and pipeline stages"""
        return {
            "tender": self.fetch_tenders(stage="tender"),
            "pipeline": self.fetch_tenders(stage="planning")  # Assuming planning stage for pipeline
        }