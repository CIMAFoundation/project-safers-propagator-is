import datetime
import logging
import os
from os.path import basename
import re
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from models.datalake import DatalakeMetadata, DatalakeResourceMetadata
from requests import HTTPError

from framework.tools import get_access_token

load_dotenv()

class DataUploadException(Exception):
    pass
class MetadataUploadException(Exception):
    pass

class MetadataDeleteException(Exception):
    pass


def delete_metadata(metadata_id):
    
    url = f'{os.getenv("CKAN_URL")}/api/action/package_delete'
    headers = {
        "Authorization": f'Bearer {access_token}',
    }
    body = {"id": metadata_id}
    try:
        response = requests.post(url, json=body, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        logging.info(f"HTTP error occurred: {http_err}")  # Python 3.6
    except Exception as err:
        logging.info(f"Other error occurred: {err}")  # Python 3.6
    else:
        logging.info("Metadata deleted")

    if response.status_code == 200:
        response_dict = response.json()
        assert response_dict["success"] is True
    else:
        logging.error('Error in deleting the metadata:')
        logging.error(response.json())
        raise MetadataDeleteException


def upload_metadata(metadata: DatalakeMetadata):
    access_token = get_access_token()

    url = f'{os.getenv("CKAN_URL")}/api/action/package_create'
    headers = {
        "Authorization": f'Bearer {access_token}',
    }
    body = metadata.as_json_dict()

    try:
        response = requests.post(url, json=body, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")  # Python 3.6
    except Exception as err:
        logging.error(f"Other error occurred: {err}")  # Python 3.6
    else:
        logging.info("Metadata uploaded")

    if response.status_code == 200:
        response_dict = response.json()
        assert response_dict["success"] is True
        created_package = response_dict["result"]
    else:
        logging.error('Error:')
        error = response.json()
        logging.error(error)
        raise MetadataUploadException(response.text)

    return created_package["id"]


def upload_resource(metadata_id: str, filepath: str, resource_metadata: DatalakeResourceMetadata, filename: str):
    access_token = get_access_token()

    url = f'{os.getenv("CKAN_URL")}/api/action/resource_create'
    logging.info(f'Uploading {filepath}')

    resource_body = resource_metadata.as_json_dict()
    headers = {
        "Authorization": f'Bearer {access_token}',
    }
    try:
        with open(filepath, "rb") as file:
            response = requests.post(url,
                        data=resource_body,
                        headers=headers,
                        files=[('upload', file)])

    except FileNotFoundError as e:
        logging.error("Error occurred: file not found" + str(filepath))
        raise DataUploadException(str(error))

    except requests.exceptions.RequestException as e:
        logging.error("Error occurred: " + str(e))
        raise DataUploadException(str(error))


    if response.status_code != 200:
        error = response.json()
        logging.error(f'Error Uploading resource: {error}')
        raise DataUploadException(str(error))

    response_json = response.json()
    resource_url = None
    
    if 'result' in response_json and 'url' in response_json['result']:
        resource_url = response_json['result']['url']
    
    return resource_url



def upload(
        metadata_id: str,
        filepath: str, 
        file_date_start:datetime, 
        file_date_end:datetime, 
        format:str='GeoJSON',
        request_code:str=None,
        datatype_resource: int = None
    ):
    
    resource_name = basename(filepath).rsplit('.', 1)[0]
    res_metadata = DatalakeResourceMetadata(
        notes=f'{resource_name}',
        package_id=metadata_id,
        file_date_start=file_date_start,
        file_date_end=file_date_end,
        name=resource_name,
        format=format,
        request_code=request_code, 
        datatype_resource=datatype_resource
    )

    logging.info(f'Uploading on metadata_id: {metadata_id}')

    # 4 . uploade datasets 
    try:
        # iterate on files inside resource_filepath and upload them
        resource_url = upload_resource(
            metadata_id,
            filepath,
            res_metadata,
            filepath
            )
        logging.info("Uploading done!")
    except DataUploadException:
        # if any error occurs in uploading datasets, delete the metadata 
        # (otherwise it will be metadata with no data)
        logging.error('One or more dataset upload failed. Removing the metadata...')
        delete_metadata(metadata_id)

    return resource_url

