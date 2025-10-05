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
from ecommerce.gcs_to_local_download import DownloadFile

fake = Faker("en_IN")

@dataclass
class Customer:
    customer_id: str
    name: str
    email: str
    gender: str
    address: str
    device_id: str
    phone: str
    city: str
    country: str
    created_at: datetime

@dataclass
class Device:
    device_id: str
    device_type: str
    os: str
    application: str
    ip_address: str
    last_login: datetime


class CustomerDataGenerator:

    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Customer data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Customer data upload to GCS completed.")
        output.close()

    def generate_customer(self, num_of_records: int):

        """ Generate customer data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting customer data generation...{date}")
        download_file = DownloadFile()

        device_file_name = f"gs://{self.bucket_name}/device_data/{datetime.now().strftime('%Y%m%d')}/devices.csv"

        device_local_path = download_file.download_from_gcs(device_file_name)
        with open(device_local_path, mode='r') as file:
            reader = csv.DictReader(file)
            devices = [Device(**row) for row in reader]


        customers = []
        for i in range(num_of_records):
            id = "CUST-" + str(uuid4())[:8]
            name = fake.name()
            email = name.split(" ")[0].lower()+"_"+str(uuid4())[:8]+"@namasteKart.com"
            gender = random.choice(["Male","Female","Other"])
            address = fake.address().replace("\n", ", ")
            device_id = random.choice(devices).device_id
            phone = fake.phone_number()
            city = fake.city()
            country = "India"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            customers.append(Customer(customer_id=id, name=name, email=email,gender=gender, address=address, device_id=device_id, phone=phone, city=city, country=country, created_at=created_at))
        rows=[]
        for row in customers:
            rows.append(asdict(row))
        file_name = "customers.csv"        
        self.__upload_to_gcs(f'customer_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows) 
        logger.info(f"Customer data generation completed. Generated {num_of_records} records.")
        return customers       
            # self.__upload_to_gcs('customer_data/customers.csv',rows)
            #return customers