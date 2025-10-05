from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
# import os
import csv
from google.cloud import storage
import io
from ride_sharing.logger import logger
from ride_sharing.gcs_to_local_download import DownloadFile
fake = Faker("en_IN")



@dataclass
class Trip:
    trip_id: str
    from_location: str
    to_locaton: str
    user_id: str
    driver_id: str
    payment_id: str
    promo_id: str
    trip_amount: float
    discount: float
    final_amount: float
    surcharge: float
    trip_date: datetime
    created_at: datetime

@dataclass
class Location:
    location_id: str
    user_id: str
    driver_id: str
    latitude: float
    longitude: float
    city: str
    state: str
    country: str
    postal_code: str
    created_at: datetime

@dataclass
class User:
    user_id: str
    name: str
    email: str
    device_id: str
    phone: str
    city: str
    state: str
    country: str
    gender: str
    created_at: datetime

@dataclass
class Driver:
    driver_id: str
    name: str
    email: str
    phone: str
    city: str
    state: str
    country: str
    id_proof: str
    license_number: str
    vehicle_id: str
    id_proof_number: str
    created_at: datetime
    
@dataclass
class PaymentMethod:
    payment_id: str
    payment_method: str
    payment_date: datetime
    status: str


@dataclass
class Promo:
    promo_id: str
    promo_code: str
    discount_type: str
    discount_value: float
    start_date: datetime
    end_date: datetime

@dataclass
class Location:
    location_id: str
    user_id: str
    driver_id: str
    latitude: float
    longitude: float
    city: str
    state: str
    country: str
    postal_code: str
    created_at: datetime


class TripDataGenerator:

    """ Class to generate trip data and upload to GCS  """

    def __init__(self, bucket_name: str = "gcs-ride-sharing-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Trip data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Trip data upload to GCS completed.")
        output.close()

    def generate_trip_data(self, num_of_records):
        """ Create the trip data"""

        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Starting trip data generation.... {date}")
        download_file = DownloadFile()
        user_file_name = f"gs://{self.bucket_name}/user_data/{datetime.now().strftime('%Y%m%d')}/users.csv"
        user_file_path = download_file.download_from_gcs(user_file_name)
        driver_file_name = f"gs://{self.bucket_name}/driver_data/{datetime.now().strftime('%Y%m%d')}/drivers.csv"
        driver_file_path = download_file.download_from_gcs(driver_file_name)
        payment_file_name = f"gs://{self.bucket_name}/payment_data/{datetime.now().strftime('%Y%m%d')}/payments.csv"
        payment_file_path = download_file.download_from_gcs(payment_file_name)
        promo_file_name = f"gs://{self.bucket_name}/promo_code/{datetime.now().strftime('%Y%m%d')}/promo_code.csv"
        promo_file_path = download_file.download_from_gcs(promo_file_name)  
        location_file_name = f"gs://{self.bucket_name}/location_data/{datetime.now().strftime('%Y%m%d')}/locations.csv"     
        location_file_path = download_file.download_from_gcs(location_file_name)  
        with open(user_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            users = [User(**row) for row in reader]        
        with open(driver_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            drivers = [Driver(**row) for row in reader]

        with open(payment_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            payments = [PaymentMethod(**row) for row in reader]

        with open(promo_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            promo_code = [Promo(**row) for row in reader]
        
        with open(location_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            locations = [Location(**row) for row in reader]

        trips =[]
        for _ in range(num_of_records):
            trip_id = "TRIP-"+str(uuid4())[:8]
            source_location_id = random.choice(locations).location_id
            destination_location_id = random.choice(locations).location_id
            user_id = random.choice(users).user_id
            driver_id = random.choice(drivers).driver_id
            payment_id = random.choice(payments).payment_id
            promo = random.choice(promo_code)
            amount = random.randint(40,2000)
            discount = (amount*float(promo.discount_value))/100 if promo.discount_type == "Percent" else float(promo.discount_value)
            # logger.info(f"Discount {discount}")
            surcharge = 0 if random.choice(["Night Charge","Peak Hour", None]) is None else random.randint(1,5)
            # logger.info(f"Surcharge {surcharge}")
            final_amount = amount - float(discount) + (amount*surcharge)
            trip_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            trips.append(
                Trip (
                    trip_id= trip_id,
                    from_location= source_location_id,
                    to_locaton=  destination_location_id,
                    user_id= user_id,
                    driver_id= driver_id,
                    payment_id= payment_id,
                    promo_id= promo.promo_id,
                    trip_amount= amount,
                    discount= discount,
                    final_amount= final_amount,
                    surcharge= surcharge,
                    trip_date= trip_date,
                    created_at= created_at
                )
            )
        rows = [asdict(trip) for trip in trips]
            
        file_name = "trip.csv"
        self.__upload_to_gcs(f'trip_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        logger.info(f"Trip data generation completed...{date}")


