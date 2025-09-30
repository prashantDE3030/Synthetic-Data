from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
import os
import csv
from google.cloud import storage
import io
from ecommerce.logger import logger

fake = Faker("en_IN")

@dataclass
class ProductView:
    view_id: str
    customer_id: str
    product_id: str
    view_date: datetime

class ProductViewDataGenerator:
    """Generates synthetic product view data and uploads it to Google Cloud Storage."""

    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    
    def __upload_to_gcs(self, destination_blob_name: str, rows):
        """Uploads a file to the GCS bucket."""
        logger.info(f"Product view data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Product view data upload to GCS completed.")
        output.close()
    
    def generate_product_views(self, num_of_records: int):
        """Generates product view data based on the number of records."""
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting product view data generation...{date}")

        product_views = []
        for _ in range(num_of_records):
            view_id = "VIEW-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            view_date = fake.date_time_between(start_date='-1y', end_date='now')

            product_view = ProductView(
                view_id=view_id,
                customer_id=customer_id,
                product_id=product_id,
                view_date=view_date
            )
            product_views.append(asdict(product_view))
        file_name = "product_views.csv"
        # Upload to GCS
        self.__upload_to_gcs(destination_blob_name=f'product_views/{datetime.now().strftime("%Y%m%d")}/{file_name}', rows=product_views)
        logger.info(f"Product view data generation completed. Total records: {num_of_records}")

        return product_views
