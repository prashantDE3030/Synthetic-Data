from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
import csv
from google.cloud import storage
import io
from ecommerce.logger import logger
from ecommerce.gcs_to_local_download import DownloadFile
fake = Faker("en_IN")


@dataclass
class Cancelation:
    cancelation_id: str
    order_id: str
    product_id: str
    customer_id: str
    reason: str
    cancelation_date: datetime
    status: str
    refund_amount: float

@dataclass
class Order:
    order_id: str
    customer_id: str
    product_id: str
    payment_id: str
    total_price: float
    order_date: datetime
    expected_delivery_date: datetime
class CancelationDataGenerator:
    """ Class to generate cancelation data and upload to GCS  """

    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Cancelation data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Cancelation data upload to GCS completed.")
        output.close()
    
    def generate_cancelation(self, num_of_records: int):
        """ Generate cancelation data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting cancelation data generation...{date}")
        download_file = DownloadFile()
        order_file_name = f'gs://{self.bucket_name}/order_data/{datetime.now().strftime("%Y%m%d")}/orders.csv'
        order_local_path = download_file.download_from_gcs(gcs_path=order_file_name)

        with open(order_local_path, mode='r') as file:
            reader = csv.DictReader(file)
            orders = [Order(**row) for row in reader]
        reasons = [
            "Found a better price elsewhere",
            "Item arrived too late",
            "Changed my mind",
            "Ordered by mistake",
            "Product no longer needed",
            "Shipping cost too high",
            "Item out of stock",
            "Other"
        ]

        statuses = "Cancelled"

        cancelations = []
        for _ in range(num_of_records):
            id = "CANC-" + str(uuid4())[:8]
            order = random.choice(orders)
            order_id = order.order_id
            product_id = order.product_id
            customer_id = order.customer_id
            reason = random.choice(reasons)
            cancelation_date = fake.date_time_between(start_date='-1y', end_date='now')
            status = random.choice(statuses)
            refund_amount = order.total_price

            cancelation = Cancelation(
                cancelation_id=id,
                order_id=order_id,
                product_id=product_id,
                customer_id=customer_id,
                reason=reason,
                cancelation_date=cancelation_date,
                status=status,
                refund_amount=refund_amount
            )
            cancelations.append(asdict(cancelation))
        rows = []
        for row in cancelations:
            rows.append(row)

        file_name = "cancelation.csv"      
        self.__upload_to_gcs(f'cancel_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        logger.info(f"Cancelation data generation completed. Generated {num_of_records} records.")

        return cancelations