from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
# import os
import csv
from google.cloud import storage
import io
from ecommerce.logger import logger
from ecommerce.gcs_to_local_download import DownloadFile
fake = Faker("en_IN")

@dataclass
class Device:
    device_id: str
    customer_id: str
    device_type: str
    os: str
    application: str
    ip_address: str
    last_login: datetime

@dataclass
class Customer:
    customer_id: str
    name: str
    email: str
    gender: str
    address: str
    phone: str
    city: str
    country: str
    created_at: datetime


class DeviceDataGenerator:
    """ Class to generate device data and upload to GCS  """

    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
    
    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Device data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Device data upload to GCS completed.")
        output.close()
    
    def generate_device(self, num_of_records: int):
        """ Generate device data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting device data generation...{date}")
        device_types = ["Mobile", "Tablet", "Desktop", "Laptop"]
        operating_systems = ["iOS", "Android", "Windows", "macOS", "Linux"]
        browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"]

        download_file = DownloadFile()

        customers_file_name = f"gs://{self.bucket_name}/customer_data/{datetime.now().strftime('%Y%m%d')}/customers.csv"

        customer_local_path = download_file.download_from_gcs(customers_file_name)
        with open(customer_local_path, mode='r') as file:
            reader = csv.DictReader(file)
            customers = [Customer(**row) for row in reader]

        devices = []
        for _ in range(num_of_records):
            id = "DEV-" + str(uuid4())[:8]
            customer_id = random.choice(customers).customer_id
            device_type = random.choice(device_types)
            os = random.choice(operating_systems)
            application = "Mobile Application" if device_type=="Mobile" else random.choice(browsers)
            ip_address = fake.ipv4_public()
            last_login = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            devices.append(Device(device_id=id, customer_id=customer_id, device_type=device_type, os=os, application=application, ip_address=ip_address, last_login=last_login))
        
        rows=[]
        for row in devices:
            rows.append(asdict(row))
        
        file_name = "devices.csv"    
        self.__upload_to_gcs(f'device_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        logger.info(f"Device data generation completed. Generated {num_of_records} records.")

        return devices