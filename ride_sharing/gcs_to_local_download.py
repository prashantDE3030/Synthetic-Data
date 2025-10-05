import os
from google.cloud import storage
from ecommerce.logger import logger




class DownloadFile:
    """Downloads files from Google Cloud Storage to local directory."""
    def __init__(self):
        self.storage_client = storage.Client()


    def download_from_gcs(self,gcs_path,local_path="tmp"):
        """ Downloads a file from the GCS bucket """

        logger.info(f"Starting download from GCS: {gcs_path}")
        parts = gcs_path.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        source_blob_name = parts[1]
        logger.info(f"GCS Bucket: {bucket_name}, Blob: {source_blob_name}")
        local_path = os.path.join(local_path, os.path.basename(source_blob_name))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        logger.info(f"Local path for download: {local_path}")
        bucket = self.storage_client.bucket(bucket_name)
        logger.info(f"Accessing bucket: {bucket.name}")
        blob = bucket.blob(source_blob_name)
        logger.info(f"Downloading blob: {blob.name}")
        blob.download_to_filename(local_path)
        logger.info(f"File {source_blob_name} downloaded from bucket {bucket_name} to {local_path}.")
        return local_path
