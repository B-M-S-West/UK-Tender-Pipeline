import requests
import json
import os
from pyairtable import Api
from dotenv import load_dotenv
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
api = Api(AIRTABLE_ACCESS_TOKEN)
table = api.table('appDayUWHh0C1xqxI', 'tblg9hC30kY7LBK8Y')
table.all()


# The bellow will need to be update into a function and params for daily
params = {
    "updatedFrom": "2025-06-01T00:00:00",
    "updatedTo": "2025-06-13T23:59:59",
    "stages": "tender",
    "limit": 5
}
url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"

try:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.RequestException as e:
    logger.error(f'Error fetching data from API: {e}')
    logger.warning("Skipping processing due to API error.")


def parse_date(date_string):
    if date_string:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00')).strftime('%m/%d/%Y')
    return None


def extract_cpv_info(tender):
    cpv_ids = []
    cpv_descs = []
    items = tender.get('items', [])
    for item in items:
        for ac in item.get('additionalClassifications', []):
            if ac.get('scheme') == 'CPV':
                cpv_ids.append(ac.get('id', ''))
                cpv_descs.append(ac.get('description', ''))
    # Remove duplicates and join as comma-separated strings
    cpv_ids = ', '.join(sorted(set(cpv_ids)))
    cpv_descs = ', '.join(sorted(set(cpv_descs)))
    return cpv_ids, cpv_descs


def extract_single_record(release):
    tender = release.get('tender', {})
    value_info =tender.get('value', {})
    tender_period = tender.get('tenderPeriod', {})
    buyer = release.get('buyer', {})
    cpv_ids, cpv_descs = extract_cpv_info(tender)
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
        "Tender End Date": parse_date(tender_period.get('endDate')), 
        "Published Date": parse_date(release.get('date')), 
        "Status": tender.get('status', ''),
        "Submission URL": tender.get('submissionMethodDetails', ''),
        "CPV Codes": cpv_ids,
        "CPV Descriptions": cpv_descs,
        "Notice URL": notice_url
    }

def upsert_tender_record(record):
    ocid = record['OCID']
    existing = table.all(formula=f"{{OCID}} = '{ocid}'")
    if existing:
        record_id = existing[0]['id']
        try:
            table.update(record_id, record)
            logger.info(f"Updated: {record['Title']}")
        except Exception as e:
            logger.error(f"Error updating {record['Title']}: {e}")
    else:
        try:
            table.create(record)
            logger.info(f"Created: {record['Title']}")
        except Exception as e:
            logger.error(f"Error creating {record['Title']}: {e}")

def process_releases(json_response):
    releases = json_response.get('releases', [])
    logger.info(f"Processing {len(releases)} releases.")
    for release in releases:
        record = extract_single_record(release)
        upsert_tender_record(record)
    logger.info("Processing complete.")

process_releases(data)