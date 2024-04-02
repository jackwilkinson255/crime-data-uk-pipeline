import os
import tempfile
import base64
import json
import functions_framework
from utils.data_police_uk_api import DataPoliceUKAPI
from utils.cloud_storage_utils import CloudStorageAPI
from utils import log

# setup logging
logger = log.setup_custom_logger('root')
dpuk = DataPoliceUKAPI()
gcs = CloudStorageAPI()


def parse_cloud_event(cloud_event):
    """Parse cloud event data to get months and crime data sets to request"""
    cloud_event_data = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    cloud_event_data = json.loads(cloud_event_data)

    months, data_sets = dpuk.validate_message(cloud_event_data)

    api_request_info = dpuk.validate_months(months)
    api_request_info["data_sets"] = dpuk.validate_crime_data_sets(data_sets)

    return api_request_info


@functions_framework.cloud_event
def run_batch_load(cloud_event):
    logger.info('Logging started')

    project = "crime-data-uk"
    raw_bucket = f"{project}-raw"
    curated_bucket = f"{project}-curated"
    api_request_info = parse_cloud_event(cloud_event)

    for interval_month in api_request_info["interval_months"]:
        file_name = f"{interval_month}.zip"
        temp_file_path = os.path.join(tempfile.gettempdir(), file_name)

        dpuk.get_zip_file(interval_month, temp_file_path)
        gcs.move_temp_file_to_bucket(raw_bucket, temp_file_path)
        gcs.extract_zip_file_to_bucket(raw_bucket, interval_month, api_request_info)
        gcs.curate_raw_data(raw_bucket, curated_bucket)

    logger.info('Cloud function complete!')



