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
class PaymentMethod:
    payment_id: str
    payment_method: str
    payment_date: datetime
    status: str

class PaymentDataGenerator:
    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Payment data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Payment data upload to GCS completed.")
        output.close()

    def generate_payment(self, num_of_records: int):
        """ Generate payment data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting payment data generation...{date}")
        payments = []
        for _ in range(num_of_records):
            id = "PAY-" + str(uuid4())[:8]
            payment_method = random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash on Delivery"])
            payment_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = random.choice(["Completed", "Pending", "Failed", "Refunded"])
            payments.append(PaymentMethod(payment_id=id,payment_method=payment_method, payment_date=payment_date, status=status))
        rows=[]
        for row in payments:
            rows.append(asdict(row))
        file_name = "payments.csv"        
        self.__upload_to_gcs(f'payment_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        logger.info(f"Payment data generation completed...{date}")
        return payments      
            # self.__upload_to_gcs('payment_data/payments.csv',rows)
            #return payments