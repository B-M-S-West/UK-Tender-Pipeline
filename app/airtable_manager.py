import os
from pyairtable import Api
from typing import List, Dict
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class AirtableManager:
    def __init__(self):
        self.airtable_token = os.getenv("AIRTABLE_ACCESS_TOKEN")
        self.base_id = os.getenv("AIRTABLE_BASE_ID")
        self.tender_table_id = os.getenv("TENDER_TABLE_ID")
        self.pipeline_table_id = os.getenv("PIPELINE_TABLE_ID")
        
        if not self.airtable_token:
            raise ValueError("AIRTABLE_ACCESS_TOKEN environment variable is required")
        
        self.api = Api(self.airtable_token)
        self.tender_table = self.api.table(self.base_id, self.tender_table_id)
        self.pipeline_table = self.api.table(self.base_id, self.pipeline_table_id) if self.pipeline_table_id else None
    
    def upsert_tender_record(self, record: Dict) -> bool:
        """Upsert a single tender record"""
        ocid = record.get('OCID')
        if not ocid:
            logger.warning("Record missing OCID, skipping")
            return False
        
        try:
            existing = self.tender_table.all(formula=f"{{OCID}} = '{ocid}'")
            if existing:
                record_id = existing[0]['id']
                self.tender_table.update(record_id, record, typecast=True)
                logger.info(f"Updated tender: {record.get('Title', 'Unknown')}")
            else:
                self.tender_table.create(record, typecast=True)
                logger.info(f"Created tender: {record.get('Title', 'Unknown')}")
            return True
        except Exception as e:
            logger.error(f"Error upserting tender {record.get('Title', 'Unknown')}: {e}")
            return False
    
    def upsert_pipeline_record(self, record: Dict) -> bool:
        """Upsert a single pipeline record"""
        if not self.pipeline_table:
            logger.error("Pipeline table not configured - set PIPELINE_TABLE_ID environment variable")
            return False
        
        ocid = record.get('OCID')
        if not ocid:
            logger.warning("Pipeline record missing OCID, skipping")
            return False
        
        try:
            existing = self.pipeline_table.all(formula=f"{{OCID}} = '{ocid}'")
            if existing:
                record_id = existing[0]['id']
                self.pipeline_table.update(record_id, record, typecast=True)
                logger.info(f"Updated pipeline: {record.get('Title', 'Unknown')}")
            else:
                self.pipeline_table.create(record, typecast=True)
                logger.info(f"Created pipeline: {record.get('Title', 'Unknown')}")
            return True
        except Exception as e:
            logger.error(f"Error upserting pipeline {record.get('Title', 'Unknown')}: {e}")
            return False
    
    def batch_upsert(self, records: List[Dict], record_type: str = "tender") -> Dict[str, int]:
        """Batch upsert records"""
        results = {"success": 0, "failed": 0}
        
        for record in records:
            if record_type == "tender":
                success = self.upsert_tender_record(record)
            else:
                success = self.upsert_pipeline_record(record)
            
            if success:
                results["success"] += 1
            else:
                results["failed"] += 1
        
        return results