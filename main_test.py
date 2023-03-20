import os
from main import CloudStorageAPI, FileMgmtUtils
import pytest
import time


BUCKET_NAME = "data-police-uk"
LATEST_DATE = "2023-01"
WORKING_DIR = os.getcwd()

gcs = CloudStorageAPI(BUCKET_NAME)
fileUtils = FileMgmtUtils(LATEST_DATE, WORKING_DIR)


@pytest.fixture
def delete_local_unzipped_file():
    fileUtils.delete_local_unzipped_file()


@pytest.fixture
def delete_blobs():
    if gcs.check_bucket_exists():
        gcs.delete_all_blobs(BUCKET_NAME)


# def test_delete_bucket(delete_blobs):
#     gcs.delete_bucket()
#     time.sleep(2)
#     assert gcs.check_bucket_exists() == False


def test_create_bucket():
    gcs.create_bucket(BUCKET_NAME)
    time.sleep(2)
    assert gcs.check_bucket_exists() == True


def test_unzip_files(delete_local_unzipped_file):
    fileUtils.unzip_file()
    EXTRACT_DIR = os.path.join(WORKING_DIR, LATEST_DATE)
    assert (os.path.exists(EXTRACT_DIR) and os.path.isdir(EXTRACT_DIR)) == True


def test_upload_directory_gcs():
    csv_path = fileUtils.concat_csvs()
    gcs.upload_file_to_gcs(csv_path)
    test_blob_name = "2023-01-street.csv"
    wait_counter = 0
    # Wait for upload as it is a large file
    while(1):
        blob_names = gcs.list_blobs(BUCKET_NAME)
        if test_blob_name in blob_names:
            blob_name_found = True
            break
        time.sleep(10)
        wait_counter+=1
        if wait_counter > 6:
            blob_name_found = False
            break

    assert blob_name_found == True
