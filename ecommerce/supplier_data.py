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
class Supplier:
    supplier_id: str
    name: str
    contact_name: str
    address: str
    phone: str
    email: str
    city: str
    country: str
    created_at: datetime

class SupplierDataGenerator:
    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()


    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Supplier data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Supplier data upload to GCS completed.")
        output.close()

        
    def generate_supplier(self, num_of_records: int):
        """ Generate supplier data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting supplier data generation...{date}")
        suppliers = []
        for i in range(num_of_records):
            id = "SUP-" + str(uuid4())[:8]
            name = fake.company()
            contact_name = fake.name()
            address = fake.address().replace("\n", ", ")
            phone = fake.phone_number()
            email = name.split(" ")[0].lower()+"_"+str(uuid4())[:8]+"@namasteKart.com"
            city = fake.city()
            country = "India"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            suppliers.append(Supplier(supplier_id=id, name=name, contact_name=contact_name, address=address, phone=phone, email=email, city=city, country=country, created_at=created_at))
        rows=[]
        for row in suppliers:
            rows.append(asdict(row))
        file_name = "suppliers.csv"        
        self.__upload_to_gcs(f'supplier_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        logger.info(f"Supplier data generation completed...{date}")
        return suppliers      
            # self.__upload_to_gcs('supplier_data/suppliers.csv',rows)
            #return suppliers

