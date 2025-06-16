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
    return Api, json, load_dotenv, os, requests


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
def _(Api, load_dotenv, os):
    load_dotenv()
    AIRTABLE_ACCESS_TOKEN = os.getenv("AIRTABLE_ACCESS_TOKEN")
    api = Api(AIRTABLE_ACCESS_TOKEN)
    table = api.table('appDayUWHh0C1xqxI', 'tblg9hC30kY7LBK8Y')
    table.all()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
