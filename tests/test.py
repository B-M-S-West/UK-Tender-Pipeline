import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import requests
    import json
    import os
    from pyairtable import Api
    from dotenv import load_dotenv
    from datetime import datetime
    return Api, datetime, json, load_dotenv, os, requests


@app.cell
def _(Api, load_dotenv, os):
    load_dotenv()
    AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
    api = Api(AIRTABLE_ACCESS_TOKEN)
    table = api.table('appDayUWHh0C1xqxI', 'tblg9hC30kY7LBK8Y')
    table.all()
    return (table,)


@app.cell
def _(json, requests):
    # Simple ocid request
    _url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages/ocds-h6vhtk-0547f4"
    _response = requests.get(_url)
    data1 = _response.json()
    data1 = json.dumps(data1)
    data1  # Display the JSON response
    return


@app.cell
def _(requests):
    params = {
        "updatedFrom": "2025-06-01T00:00:00",
        "updatedTo": "2025-06-13T23:59:59",
        "stages": "tender",
        "limit": 5
    }
    _url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
    _response = requests.get(_url, params=params)
    data_2 = _response.json()
    data_2
    return (data_2,)


@app.cell
def _(data_2):
    def extract_tender_info(release):
        tender = release.get('tender', {})
        return {
            "OCID": release.get("ocid"),
            "Title": tender.get("title"),
            "Status": tender.get("status"),
            "Value": tender.get("value", {}).get("amount"),
            "Buyer": release.get("buyer", {}).get("name"),
            "Submission_Deadline": tender.get("submissionMethodDetails"),
        }

    # Test on a sample release
    sample_release = data_2['releases'][0]
    extract_tender_info(sample_release)
    return


@app.cell
def _(data_2):
    sample_release_1 = data_2['releases']
    sample_release_1
    return


@app.cell
def _(datetime):
    def parse_date(date_string):
        if date_string:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00')).strftime('%m/%d/%Y')
        return None
    return (parse_date,)


@app.function
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


@app.cell
def _(parse_date):
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
    return (extract_single_record,)


@app.cell
def _(table):
    def upsert_tender_record(record):
        ocid = record['OCID']
        existing = table.all(formula=f"{{OCID}} = '{ocid}'")
        if existing:
            record_id = existing[0]['id']
            try:
                table.update(record_id, record)
                print(f"Updated: {record['Title']}")
            except Exception as e:
                print(f"Error updating {record['Title']}: {e}")
        else:
            try:
                table.create(record)
                print(f"Created: {record['Title']}")
            except Exception as e:
                print(f"Error creating {record['Title']}: {e}")
    return (upsert_tender_record,)


@app.cell
def _(extract_single_record, upsert_tender_record):
    def process_releases(json_response):
        releases = json_response.get('releases', [])
        for release in releases:
            record = extract_single_record(release)
            upsert_tender_record(record)
    return (process_releases,)


@app.cell
def _(data_2, process_releases):
    process_releases(data_2)
    return


if __name__ == "__main__":
    app.run()
