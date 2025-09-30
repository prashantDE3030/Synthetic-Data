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
class Return:
    return_id: str
    order_id: str
    product_id: str
    customer_id: str
    reason: str
    return_date: datetime
    status: str

class ReturnDataGenerator:
    """ Class to generate return data and upload to GCS  """
    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
    
    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Return data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Return data upload to GCS completed.")
        output.close()
    
    def generate_return(self, num_of_records: int):
        """ Generate return data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting return data generation...{date}")

        returns = []
        for _ in range(num_of_records):
            id = "RET-" + str(uuid4())[:8]
            order_id = "ORD-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            reason = random.choice(["Defective", "Wrong Item", "Changed Mind", "Better Price Elsewhere", "No Longer Needed"])
            return_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "Returned"
            returns.append(Return(return_id=id, order_id=order_id, product_id=product_id, customer_id=customer_id, reason=reason, return_date=return_date, status=status))
        rows=[]
        for row in returns:
            rows.append(asdict(row))
        file_name = "returns.csv"        
        self.__upload_to_gcs(f'return_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)

        return returns