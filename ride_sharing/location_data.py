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

state_city_map = {
    "Andhra Pradesh": ["Visakhapatnam", "Vijayawada", "Guntur", "Nellore"],
    "Arunachal Pradesh": ["Itanagar", "Naharlagun"],
    "Assam": ["Guwahati", "Silchar", "Dibrugarh"],
    "Bihar": ["Patna", "Gaya", "Muzaffarpur"],
    "Chhattisgarh": ["Raipur", "Bhilai", "Bilaspur"],
    "Goa": ["Panaji", "Margao", "Vasco da Gama"],
    "Gujarat": ["Ahmedabad", "Surat", "Vadodara", "Rajkot"],
    "Haryana": ["Gurugram", "Faridabad", "Panipat"],
    "Himachal Pradesh": ["Shimla", "Dharamshala", "Manali"],
    "Jharkhand": ["Ranchi", "Jamshedpur", "Dhanbad"],
    "Karnataka": ["Bengaluru", "Mysuru", "Mangalore", "Hubli"],
    "Kerala": ["Thiruvananthapuram", "Kochi", "Kozhikode"],
    "Madhya Pradesh": ["Bhopal", "Indore", "Gwalior", "Jabalpur"],
    "Maharashtra": ["Mumbai", "Pune", "Nagpur", "Nashik"],
    "Manipur": ["Imphal"],
    "Meghalaya": ["Shillong"],
    "Mizoram": ["Aizawl"],
    "Nagaland": ["Kohima", "Dimapur"],
    "Odisha": ["Bhubaneswar", "Cuttack", "Rourkela"],
    "Punjab": ["Amritsar", "Ludhiana", "Jalandhar", "Patiala"],
    "Rajasthan": ["Jaipur", "Udaipur", "Jodhpur", "Kota"],
    "Sikkim": ["Gangtok"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai", "Tiruchirappalli"],
    "Telangana": ["Hyderabad", "Warangal", "Nizamabad"],
    "Tripura": ["Agartala"],
    "Uttar Pradesh": ["Lucknow", "Kanpur", "Varanasi", "Agra", "Noida"],
    "Uttarakhand": ["Dehradun", "Haridwar", "Haldwani"],
    "West Bengal": ["Kolkata", "Siliguri", "Howrah", "Durgapur"],
    "Delhi": ["New Delhi", "Dwarka", "Saket"],
    "Jammu and Kashmir": ["Srinagar", "Jammu", "Anantnag"],
    "Ladakh": ["Leh", "Kargil"],
    "Chandigarh": ["Chandigarh"],
    "Puducherry": ["Puducherry", "Karaikal"],
}


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

class LocationDataGenerator:
    """ Class to generate location data and upload to GCS  """

    def __init__(self, bucket_name: str = "gcs-ride-sharing-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
    
    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Location data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Location data upload to GCS completed.")
        output.close()
    
    def generate_location(self, num_of_records: int):
        """ Generate location data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting location data generation...{date}")
        download_file = DownloadFile()
        user_file_name = f"gs://{self.bucket_name}/user_data/{datetime.now().strftime('%Y%m%d')}/users.csv"
        driver_file_name = f"gs://{self.bucket_name}/driver_data/{datetime.now().strftime('%Y%m%d')}/drivers.csv"
        user_local_path = download_file.download_from_gcs(user_file_name)
        driver_file_path = download_file.download_from_gcs(driver_file_name)
        with open(user_local_path, mode='r') as file:
            reader = csv.DictReader(file)
            users = [User(**row) for row in reader]
        with open(driver_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            drivers = [Driver(**row) for row in reader]        
        
        locations = []
        for _ in range(num_of_records):
            location_id = "LOC-" + str(uuid4())[:8]
            user_id = random.choice(users).user_id
            driver_id = random.choice(drivers).driver_id
            latitude = fake.latitude()
            longitude = fake.longitude()
            state = random.choice(list(state_city_map.keys()))
            city = random.choice(state_city_map[state])
            country = "India"
            postal_code = fake.postcode()
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            locations.append(Location(location_id=location_id, user_id=user_id, driver_id=driver_id, latitude=latitude, longitude=longitude, city=city, state=state, country=country, postal_code=postal_code, created_at=created_at))
        
        rows=[]
        for row in locations:
            rows.append(asdict(row))
        
        file_name=f"location_data/{datetime.now().strftime('%Y%m%d')}/locations.csv"
        self.__upload_to_gcs(file_name,rows)
        logger.info(f"Location data generation completed...{date}")