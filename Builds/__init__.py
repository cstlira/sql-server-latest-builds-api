import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, PublicAccess
import json
import pandas
import os

SAS_TOKEN = os.environ.get('SAS_TOKEN')
blob_service_client = BlobServiceClient(account_url = SAS_TOKEN)
latest_builds_file = 'builds.json'
container_name = 'sqlserverbuildsapi'

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    json = download_file_from_blob_storage(container_name, latest_builds_file)

    if json:
        return func.HttpResponse(json)
    else:
        return func.HttpResponse(
             "Error returning last SQL Server patches.",
             status_code=400
        )

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