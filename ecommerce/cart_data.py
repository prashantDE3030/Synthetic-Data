from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
import csv
from google.cloud import storage
import io
from ecommerce.logger import logger

fake = Faker("en_IN")

@dataclass
class Cart:
    cart_id: str
    customer_id: str
    product_id: str
    quantity: int
    added_at: datetime


class CartDataGenerator:
    """ Class to generate cart data and upload to GCS  """

    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
    

    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Cart data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Cart data upload to GCS completed.")
        output.close()
    
    def generate_cart(self, num_of_records: int):
        """ Generate cart data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting cart data generation...{date}")
        carts = []
        for _ in range(num_of_records):
            id = "CART-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            quantity = random.randint(1, 5)
            added_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            carts.append(Cart(cart_id=id, customer_id=customer_id, product_id=product_id, quantity=quantity, added_at=added_at))
        rows=[]
        for row in carts:
            rows.append(asdict(row))
        file_name = "carts.csv"        
        self.__upload_to_gcs(f'cart_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)  
        logger.info(f"Cart data generation completed. Generated {num_of_records} records.")
        return carts