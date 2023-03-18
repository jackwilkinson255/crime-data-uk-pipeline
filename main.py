from google.cloud import storage
from zipfile import ZipFile
import os
import shutil
import pandas as pd
import wget


class DataPoliceUKAPI():
    """Get latest police UK data in a batch/zip file"""
    def get_latest_zip_file(self):
        wget.download('https://data.police.uk/data/archive/latest.zip')


class CloudStorageAPI():

    def __init__(self, bucket_name) -> None:
        self.bucket_name = bucket_name
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


    def delete_bucket(self):
        """Deletes a bucket. The bucket must be empty."""
        if self.bucket_name in self.list_buckets():
            bucket = self.storage_client.get_bucket(self.bucket_name)
            bucket.delete()
            print(f"Bucket {self.bucket_name} deleted")
        else:
            print(f"Bucket {self.bucket_name} does not exist")


    def check_bucket_exists(self) -> bool:
        if self.bucket_name in self.list_buckets():
            return True
        return False


    def create_bucket(self, bucket_name, storage_class="STANDARD", storage_loc="EUROPE-WEST2"):
        if bucket_name in self.list_buckets():
            print(f"Bucket {bucket_name} already created")
        else:
            bucket = self.storage_client.bucket(bucket_name)
            bucket.storage_class = storage_class
            self.storage_client.create_bucket(bucket, location=storage_loc)
            print(f"Bucket {bucket_name} created")


    def delete_blob(self, bucket_name, blob_name):
        """Deletes a blob from the bucket."""
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print(f"Blob {blob_name} deleted.")


    def delete_all_blobs(self, bucket_name):
        blob_names = self.list_blobs(bucket_name)

        if blob_names:
            for blob in blob_names:
                self.delete_blob(bucket_name, blob)

            print(f"All blobs in {bucket_name} bucket deleted.")

        else:
            print(f"{bucket_name} bucket already empty.")


    def get_bucket_object(self, bucket_name):
        return self.storage_client.get_bucket(bucket_name)


    def upload_file_to_gcs(self, file_path):
        assert os.path.isfile(file_path)
        print("Uploading to GCS...")

        bucket = self.get_bucket_object(self.bucket_name)
        blob = bucket.blob(os.path.basename(file_path))
        blob.upload_from_filename(file_path)
        print("Upload complete!")


    def list_blobs(self, bucket_name):
        """Lists all the blobs in the bucket."""
        blobs = self.storage_client.list_blobs(bucket_name)
        blob_names = []
        for blob in blobs:
            blob_names.append(blob.name)

        return blob_names



class FileMgmtUtils():

    def __init__(self, query_date, working_dir) -> None:
        self.query_date = query_date
        self.working_dir = working_dir
        self.extract_dir  = os.path.join(working_dir, query_date)


    def delete_local_unzipped_file(self):
        if os.path.exists(self.extract_dir) and os.path.isdir(self.extract_dir):
            shutil.rmtree(self.extract_dir)


    def unzip_file(self):
        """Extract all street-outcome CSVs for a particular month"""
        self.zip_file_path = os.path.join(self.working_dir, "latest.zip")
        with ZipFile(self.zip_file_path, 'r') as zip_file:
            for file in zip_file.namelist():
                if file.startswith(f'{self.query_date}') and file.endswith('street.csv'):
                    zip_file.extract(file, self.working_dir)
                    print(f"File '{file}' extracted")

            # Remove __MACOSX directory
            macosx_dir  = os.path.join(self.working_dir, "__MACOSX")
            if os.path.exists(macosx_dir) and os.path.isdir(macosx_dir):
                shutil.rmtree(macosx_dir)


    def concat_csvs(self):
        """Concatenate all of these CSVs into a single data frame and save as CSV"""
        df_list = []
        concat_csv_path = os.path.join(self.working_dir, f'{self.query_date}-street.csv')
        if not os.path.exists(concat_csv_path):
            for file in os.listdir(self.extract_dir):
                df = pd.read_csv(f'{self.extract_dir}/{file}', index_col=None, header=0)
                df_list.append(df)

            concat_frame = pd.concat(df_list, axis=0, ignore_index=True)
            concat_frame.to_csv(concat_csv_path,index=False)
        return concat_csv_path



def run_app():

    BUCKET_NAME = "data-police-uk-hastings"
    LATEST_DATE = "2023-01"
    WORKING_DIR = os.getcwd()

    dpuk = DataPoliceUKAPI()
    # dpuk.get_latest_zip_file()

    gcs = CloudStorageAPI(BUCKET_NAME)
    fileUtils = FileMgmtUtils(LATEST_DATE, WORKING_DIR)

    fileUtils.delete_local_unzipped_file()
    if gcs.check_bucket_exists():
        gcs.delete_all_blobs(BUCKET_NAME)
    # gcs.delete_bucket()

    # gcs.create_bucket(BUCKET_NAME)
    fileUtils.unzip_file()
    csv_path = fileUtils.concat_csvs()
    gcs.upload_file_to_gcs(csv_path)


if __name__ == "__main__":
    run_app()
