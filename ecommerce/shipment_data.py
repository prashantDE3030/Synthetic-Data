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
class Shipment:
    shipment_id: str
    order_id: str
    shipment_date: datetime
    delivery_date: datetime
    status: str
    tracking_number: str

class ShipmentDataGenerator:
    """Generates synthetic shipment data."""
    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    
    def __upload_to_gcs(self, destination_blob_name: str, rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Shipment data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Shipment data upload to GCS completed.")
        output.close()
    
    def generate_shipment(self, num_of_records: int):
        """ Generate shipment data bases on the number of records """

        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting shipment data generation...{date}")
        shipments = []
        for i in range(num_of_records):
            id = "SHIP-" + str(uuid4())[:8]
            order_id = "ORD-" + str(uuid4())[:8]
            shipment_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            delivery_date = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            status = random.choice(["Shipped", "In Transit", "Delivered", "Delayed", "Cancelled"])
            tracking_number = str(uuid4()).replace("-", "").upper()[:12]
            shipments.append(Shipment(shipment_id=id, order_id=order_id, shipment_date=shipment_date, delivery_date=delivery_date, status=status, tracking_number=tracking_number))
        rows=[]
        for row in shipments:
            rows.append(asdict(row))
        file_name = "shipments.csv"        
        self.__upload_to_gcs(f'shipment_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        logger.info(f"Shipment data generation completed. Generated {num_of_records} records.")
        return shipments 