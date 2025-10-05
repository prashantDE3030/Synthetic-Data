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
class Vehical:
    vehicle_id: str
    make: str
    model: str
    year: int
    color: str
    license_plate: str
    vehicle_type: str
    created_at: datetime

class VehicalDataGenerator:
    """ Class to generate user data and upload to GCS """

    def __init__(self, bucket_name: str = "gcs-ride-sharing-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    
    def __upload_to_gcs(self, destination_blob_name: str, rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"User data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"User data upload to GCS completed.")
        output.close()
    
    def generate_vehical(self, num_of_records: int):
        """ Generate user data based on the number of records """
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting vehical data generation...{date}")

        vehicals = []
        for _ in range(num_of_records):
            vehicle_id = "VEH-" + str(uuid4())[:8]
            make = random.choice(["Toyota", "Honda", "Skoda", "Mahindra", "BMW", "Maruti Suzuki", "Mercedes", "Audi", "Hyundai", "Kia"])
            model = random.choice(["Model A", "Model B", "Model C", "Model D", "Model E"])
            year = random.randint(2010, 2025)
            color = random.choice(["Red", "Blue", "Green", "Black", "White", "Silver", "Gray", "Yellow"])
            license_plate = fake.state()[:2].upper()+''.join(random.choices('0123456789', k=2)) +''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))+''.join(random.choices('0123456789', k=4))
            vehicle_type = random.choice(["Sedan", "SUV", "Hatchback", "Auto", "Bike", "Van"])
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            vehical = Vehical(
                vehicle_id=vehicle_id,
                make=make,
                model=model,
                year=year,
                color=color,
                license_plate=license_plate,
                vehicle_type=vehicle_type,
                created_at=created_at
            )
            vehicals.append(asdict(vehical))
        
        file_name = "vehicals.csv"
        self.__upload_to_gcs(f"vehical_data/{datetime.now().strftime('%Y%m%d')}/{file_name}", vehicals)
        logger.info(f"Vehical data generation completed. Generated {num_of_records} records.")