import datetime
import logging
import azure.functions as func
import requests
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, PublicAccess
import pandas
import json

SAS_TOKEN = os.environ.get('SAS_TOKEN')

blob_service_client = BlobServiceClient(account_url = SAS_TOKEN)

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    csv_url = 'https://docs.google.com/spreadsheets/d/16Ymdz80xlCzb6CwRFVokwo0onkofVYFoSkc7mYe6pgw/export?gid=0&format=csv'
    csv = get_builds_csv(csv_url)
    container_name = 'sqlserverbuildsapi'
    blob_name = 'builds.csv'
    
    ## Stores the CSV file if it was successfully retrieved, else we get the last available in the container.
    if csv: 
        try:
            upload_file_to_blob_storage(csv,container_name,blob_name) 
        except Exception as e:
            logging.error(e)
    else:
        csv = download_file_from_blob_storage(container_name, blob_name)

    try:
        csv_data = str(csv,'utf-8')
        csv = StringIO(csv_data)
        latest_builds = generate_latest_builds_json(csv)
        upload_file_to_blob_storage(latest_builds, container_name, 'builds.json')
    except Exception as e:
        logging.error(e)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

def get_builds_csv(url):
    """Download the CSV from the Google Sheets file."""

    resp = requests.get(url)
    if resp.status_code != 200:
        logging.error("Error obtaining csv.")
    else:
        logging.info("CSV successfully obtained!")
        return resp.content

def upload_file_to_blob_storage(file,container,blob):
    """Uploads a file to the blob storage container """
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=blob)
        blob_client.upload_blob(file, overwrite=True)
    except Exception as e:
        logging.error(e)
    else:
        logging.info(f"We've uploaded the file '{blob}' successfully!")
        return True


def download_file_from_blob_storage(container,blob):
    """Download a file from the blob storage container """
    try:
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob)
        file = blob_client.download_blob().readall()
    except Exception as e:
        logging.error(e)
    else:
        logging.info(f"We've downloaded the file '{blob}' successfully!")
        return file

def generate_latest_builds_json(csv=None):
    try:
        builds = pandas.read_csv(csv)
        patches = builds[(builds['CTP'] != True) & (builds['RTM'] != True) & (builds['Version'] >= 8)]
        latest_versions = patches.groupby("SQLServer")["Build"].max()
        details = patches[['SQLServer','Version','Build','ReleaseDate', 'Link', 'FileVersion']].sort_values(['Version'])
        result = pandas.merge(latest_versions, details, on=['Build'], how="inner").sort_values('Version').to_json(orient='records')
    except Exception as e:
        logging.error(e)
        logging.error("Error generating the JSON file")
    else:
        logging.info(f"Successfully generated the latest builds JSON file.")
        return result