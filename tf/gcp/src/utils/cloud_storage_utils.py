import os
import io
from google.cloud import storage
import logging
from zipfile import ZipFile
import pandas as pd

logger = logging.getLogger('root')

class CloudStorageAPI():

    def __init__(self) -> None:
        self.storage_client = storage.Client()


    def list_buckets(self) -> object:
        """Lists all buckets."""
        self.storage_client = storage.Client()
        buckets = self.storage_client.list_buckets()
        bucket_names = []
        if buckets:
            for bucket in buckets:
                bucket_names.append(bucket.name)
            return bucket_names
        else:
            return []


    def delete_bucket(self, bucket_name):
        """Deletes a bucket. The bucket must be empty."""
        if bucket_name in self.list_buckets():
            bucket = self.storage_client.get_bucket(bucket_name)
            bucket.delete()
            logger.info(f"Bucket {bucket_name} deleted")
        else:
            logger.info(f"Bucket {bucket_name} does not exist")


    def check_bucket_exists(self, bucket_name) -> bool:
        if bucket_name in self.list_buckets():
            return True
        return False


    def create_bucket(self, bucket_name, storage_class="STANDARD", storage_loc="EUROPE-WEST2"):
        if bucket_name in self.list_buckets():
            logger.info(f"Bucket {bucket_name} already created")
        else:
            bucket = self.storage_client.bucket(bucket_name)
            bucket.storage_class = storage_class
            self.storage_client.create_bucket(bucket, location=storage_loc)
            logger.info(f"Bucket {bucket_name} created")


    def delete_blob(self, bucket_name, blob_name):
        """Deletes a blob from the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        logger.info(f"Blob {blob_name} deleted.")


    def delete_all_blobs(self, bucket_name):
        blob_names = self.list_blobs(bucket_name)
        if blob_names:
            for blob in blob_names:
                self.delete_blob(bucket_name, blob)

            logger.info(f"All blobs in {bucket_name} bucket deleted.")
        else:
            logger.info(f"{bucket_name} bucket already empty.")


    def get_bucket_object(self, bucket_name):
        return self.storage_client.get_bucket(bucket_name)


    def upload_file_to_gcs(self, bucket_name, file_path):
        if not os.path.isfile(file_path):
            logger.error("File does not exist!")
            raise FileExistsError
        logger.info("Uploading to GCS...")
        bucket = self.get_bucket_object(bucket_name)
        blob = bucket.blob(os.path.basename(file_path))
        blob.upload_from_filename(file_path)
        logger.info("Upload complete!")


    def move_temp_file_to_bucket(self, bucket_name, temp_blob_path):
        """
        Extract zip file stored in tmp directory, move to gcs bucket
        unzip required files to separate directory
        """
        logging.info("Extracting temp zip file to gcs bucket")
        bucket = self.storage_client.get_bucket(bucket_name)
        zip_blob = bucket.blob(os.path.basename(temp_blob_path))
        zip_blob.upload_from_filename(temp_blob_path)


    def extract_zip_file_to_bucket(self, bucket_name, query_month, api_request_info):
        """Unzip the file and load contents into raw bucket"""
        self.api_request_info = api_request_info
        bucket = self.storage_client.get_bucket(bucket_name)
        zip_blob = bucket.blob(f"{query_month}.zip")

        logging.info("Extracting zip to GCS directories")
        with zip_blob.open(mode='rb') as zip_archive_file:
            with ZipFile(zip_archive_file) as zip_file:
                for file in zip_file.namelist():
                    # Only save an extracted file if it has our required month and data set
                    if self.target_month(file) and self.target_data_set(file):
                        self.extract_blob(bucket, file, zip_file)


    def target_month(self, file):
        target_month = False
        for month in self.api_request_info["record_months"]:
            if file.startswith(month):
                target_month = True
                break
        return target_month


    def target_data_set(self, file):
        target_data_set = False
        for data_set in self.api_request_info["data_sets"]:
            if file.endswith(f"{data_set}.csv"):
                target_data_set = True
                break
        return target_data_set


    def extract_blob(self,bucket, file, zip_file):
        zip_blob = bucket.blob(file)
        zip_blob.upload_from_string(zip_file.read(file))
        logger.info(f"File '{file}' extracted")


    def curate_raw_data(self, raw_bucket, dest_bucket):
        """Concatenate all of these CSVs into a single data frame and save as CSV"""
        self.dest_bucket = dest_bucket
        self.raw_bucket = raw_bucket

        self.df_dict = self.make_month_data_set_dict()
        self.populate_month_data_set_dict()
        self.concat_month_data_set_dict()


    def make_month_data_set_dict(self):
        df_dict = {}
        # Create a key with an empty list for each month and crime type combination
        for record_month in self.api_request_info["record_months"]:
            for crime_type in self.api_request_info["data_sets"]:
                df_dict[f"{record_month}-{crime_type}"] = []
        logger.info(f"df_dict: {df_dict}")
        return df_dict


    def populate_month_data_set_dict(self):
        # Loop through all the blobs in the raw bucket
        logger.info("Loop through all the blobs in the raw bucket")
        raw_blobs = self.storage_client.list_blobs(self.raw_bucket)

        for blob in raw_blobs:
            month, data_set = self.get_blob_month_and_data_set(blob)
            if self.target_month_and_data_set(month, data_set):
                self.append_to_df_dict(blob, month, data_set)


    def list_blobs(self, bucket_name):
        """Lists all the blobs in the bucket."""
        blobs = self.storage_client.list_blobs(bucket_name)
        blob_names = []
        for blob in blobs:
            blob_names.append(blob.name)
        return blob_names


    def get_blob_month_and_data_set(self, blob):
        blob_name = blob.name
        file_name = os.path.basename(blob_name).split('.')[0]
        file_month = file_name[:7]
        data_set = file_name.split('-')[-1]
        data_set = 'stop-and-search' if data_set == 'search' else data_set

        return file_month, data_set


    def target_month_and_data_set(self, file_month, data_set):
        return file_month in self.api_request_info["record_months"] \
                and data_set in self.api_request_info["data_sets"]


    def append_to_df_dict(self, blob, file_month, data_set):
        csv_data = blob.download_as_string()
        df_csv_data = pd.read_csv(io.StringIO(csv_data.decode('utf-8')), index_col=None, header=0)
        self.df_dict[f"{file_month}-{data_set}"].append(df_csv_data)


    def concat_month_data_set_dict(self):
        logger.info("Looping through dict of CSVs and concatenating")
        for file_name, df_list in self.df_dict.items():
            if df_list:
                concat_frame = pd.concat(df_list, axis=0, ignore_index=True)
                csv_buffer = pd.DataFrame.to_csv(concat_frame, index=False)
                curated_bucket = self.storage_client.get_bucket(self.dest_bucket)
                data_set = file_name.split('-')[-1]
                data_set = 'stop-and-search' if data_set == 'search' else data_set
                csv_blob = curated_bucket.blob(f"{data_set}/{file_name}.csv")
                csv_blob.upload_from_string(csv_buffer)


    def download_blob_to_file(self, bucket, blob_name, path):
        bucket = self.storage_client.get_bucket(bucket)
        blob = bucket.blob(blob_name)
        if not os.path.exists(path):
            os.makedirs(path)
        blob.download_to_filename(f"{path}/{blob_name}")





