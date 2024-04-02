import pytest
import os
import shutil
import pandas as pd
from tf.gcp.src.utils.data_police_uk_api import DataPoliceUKAPI
from tf.gcp.src.utils.cloud_storage_utils import CloudStorageAPI
from tf.gcp.src.utils import log

# setup logging
logger = log.setup_custom_logger('root')
dpuk = DataPoliceUKAPI()
gcs = CloudStorageAPI()

project = "crime-data-uk"
raw_bucket = f"{project}-raw-test"
curated_bucket = f"{project}-curated-test"


def parse_cloud_event(cloud_event_data):
    """Parse cloud event data to get months and crime data sets to request"""
    # cloud_event_data = base64.b64decode(cloud_event.data["message"]["data"]).decode() # Not needed for testing
    # cloud_event_data = json.loads(cloud_event_data) # Not needed for testing
    months, data_sets = dpuk.validate_message(cloud_event_data)
    api_request_info = dpuk.validate_months(months)
    api_request_info["data_sets"] = dpuk.validate_crime_data_sets(data_sets)

    return api_request_info


def clean_up_gcs(buckets):
    for bucket in buckets:
        gcs.delete_all_blobs(bucket)
        gcs.delete_bucket(bucket)


def clean_up_local(dir, zip_dir):
    if os.path.exists(dir):
        shutil.rmtree(dir)
    logger.info(f"{dir} directory removed")

    if os.path.exists(zip_dir):
        os.unlink(zip_dir)


def compare_csvs(csv1_path, csv2_path):
    df1 = pd.read_csv(csv1_path)
    df2 = pd.read_csv(csv2_path)

    # Compare the two dataframes
    if df1.equals(df2):
        return True
    else:
        return False


@pytest.fixture
def create_test_buckets():
    gcs.create_bucket(raw_bucket, storage_loc='US-CENTRAL1')
    gcs.create_bucket(curated_bucket, storage_loc='US-CENTRAL1')


@pytest.fixture
def create_zip_file():
    dir_name = 'tests/data/2023-03'
    shutil.make_archive(dir_name, 'zip', dir_name)


@pytest.fixture
def upload_zip_to_gcs():
    dir_name = 'tests/data/2023-03.zip'
    gcs.upload_file_to_gcs(raw_bucket, dir_name)


def test_run_batch_load(create_test_buckets, create_zip_file, upload_zip_to_gcs):
    """Start the test assuming we have already loaded the zip file into the raw bucket."""
    logger.info('Logging started')
    data_msg = {"months": ["2023-03", "2021-12", "2020-04"], "data_sets": ["street", "outcomes"]}
    api_request_info = parse_cloud_event(data_msg)

    logger.info(f"api_request_data['interval_months']: {api_request_info['interval_months']}")

    for interval_month in api_request_info["interval_months"]:
        # file_name = f"{interval_month}.zip"
        # temp_file_path = os.path.join(tempfile.gettempdir(), file_name)
        # dpuk.get_zip_file(interval_month, temp_file_path)
        # gcs.move_temp_file_to_bucket(raw_bucket, temp_file_path)
        gcs.extract_zip_file_to_bucket(raw_bucket, interval_month, api_request_info)
        gcs.curate_raw_data(raw_bucket, curated_bucket)

        # get blobs from curated test bucket
        blob_names = gcs.list_blobs(curated_bucket)

        # download blobs to local csv files
        for blob in blob_names:
            print(f"blob: {blob}")
            actual_path = f"tests/data/actual/"
            gcs.download_blob_to_file(curated_bucket, blob, actual_path)

            assert compare_csvs(f"{actual_path}/{blob}", f"tests/data/expected/{blob}") == True

        # Clean up after test has finished
        # clean_up_local(actual_path, zip_path='tests/data/2023-03.zip')
        # clean_up_gcs([raw_bucket, curated_bucket])





