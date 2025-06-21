from datetime import datetime
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class TenderDataProcessor:
    @staticmethod
    def parse_date(date_string: str) -> str:
        """Parse ISO date string to MM/DD/YYYY format"""
        if date_string:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00')).strftime('%m/%d/%Y')
        return None

    @staticmethod
    def extract_cpv_info(tender: Dict) -> tuple:
        """Extract main and additional CPV codes and descriptions"""
        cpv_ids = []
        cpv_descs = []
        
        # Main CPV classification
        main_cpv = tender.get("classification")
        if main_cpv.get('scheme') == 'CPV':
            cpv_ids.append(main_cpv.get('id', ''))
            cpv_descs.append(main_cpv.get('description', ''))
        
        # Additional CPV classifications
        items = tender.get('items', [])
        for item in items:
            for ac in item.get('additionalClassifications', []):
                if ac.get('scheme') == 'CPV':
                    cpv_ids.append(ac.get('id', ''))
                    cpv_descs.append(ac.get('description', ''))
        
        cpv_ids = sorted(set(filter(None, cpv_ids)))
        cpv_descs = sorted(set(filter(None, cpv_descs)))
        return cpv_ids, cpv_descs

    @staticmethod
    def extract_tender_record(release: Dict) -> Dict:
        """Extract tender record for main table"""
        tender = release.get('tender', {})
        value_info = tender.get('value', {})
        tender_period = tender.get('tenderPeriod', {})
        buyer = release.get('buyer', {})
        cpv_ids, cpv_descs = TenderDataProcessor.extract_cpv_info(tender)
        release_id = release.get('id', '')
        notice_url = f"https://www.find-tender.service.gov.uk/Notice/{release_id}" if release_id else ''
        
        return {
            "OCID": release.get('ocid'),
            "Release ID": release_id,
            "Title": tender.get('title', ''),
            "Description": tender.get('description', ''),
            "Buyer Name": buyer.get('name', ''),
            "Value Amount": value_info.get('amount', '') if value_info else None,
            "Currency": value_info.get('currency', '') if value_info else '',
            "Tender End Date": TenderDataProcessor.parse_date(tender_period.get('endDate')), 
            "Published Date": TenderDataProcessor.parse_date(release.get('date')), 
            "Status": tender.get('status', ''),
            "Submission URL": tender.get('submissionMethodDetails', ''),
            "CPV Codes": cpv_ids,
            "CPV Descriptions": cpv_descs,
            "Notice URL": notice_url,
        }
    
    @staticmethod
    def extract_pipeline_record(release: Dict) -> Dict:
        """Extract pipeline record for pipeline table"""
        planning = release.get('planning', {})
        tender = release.get('tender', {})
        buyer = release.get('buyer', {})
        budget = planning.get('budget', {})
        
        return {
            "OCID": release.get('ocid'),
            "Release ID": release.get('id', ''),
            "Title": tender.get('title', '') or planning.get('project', {}).get('title', ''),
            "Description": tender.get('description', '') or planning.get('project', {}).get('description', ''),
            "Buyer Name": buyer.get('name', ''),
            "Budget Amount": budget.get('amount', {}).get('amount', '') if budget.get('amount') else None,
            "Currency": budget.get('amount', {}).get('currency', '') if budget.get('amount') else '',
            "Planning Stage": planning.get('project', {}).get('sector', ''),
            "Published Date": TenderDataProcessor.parse_date(release.get('date')),
            "Status": "Pipeline",
            "Last Updated": datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        }

    @staticmethod
    def process_releases(json_response: Dict, record_type: str = "tender") -> List[Dict]:
        """Process releases and return list of records"""
        if not json_response:
            return []
        
        releases = json_response.get('releases', [])
        logger.info(f"Processing {len(releases)} {record_type} releases")
        
        records = []
        for release in releases:
            try:
                if record_type == "tender":
                    record = TenderDataProcessor.extract_tender_record(release)
                else:
                    record = TenderDataProcessor.extract_pipeline_record(release)
                records.append(record)
            except Exception as e:
                logger.error(f"Error processing release {release.get('id', 'unknown')}: {e}")
        
        return records