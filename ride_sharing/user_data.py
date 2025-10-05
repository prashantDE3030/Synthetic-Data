from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
import os
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
class Device:
    device_id: str
    device_type: str
    os: str
    application: str
    ip_address: str
    last_login: datetime


class UserDataGenerator:
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
    
    def generate_user(self, num_of_records: int):
        """ Generate user data based on the number of records """
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting user data generation...{date}")
        download_file = DownloadFile()
        device_fie_name = f"gs://{self.bucket_name}/device_data/{datetime.now().strftime('%Y%m%d')}/devices.csv"

        device_local_path = download_file.download_from_gcs(device_fie_name)
        with open(device_local_path, mode='r') as file:
            reader = csv.DictReader(file)
            devices = [Device(**row) for row in reader]        

        users = []
        for i in range(num_of_records):
            id = "USER-" + str(uuid4())[:8]
            name = fake.name()
            email = name.split(" ")[0].lower() + "_" + str(uuid4())[:8] + "@rideShare.com"
            phone = fake.phone_number()
            device_id = random.choice(devices).device_id
            state = random.choice(list(state_city_map.keys()))
            city = random.choice(state_city_map[state])
            country = "India"
            gender = random.choice(["Male","Female","Other"])
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            users.append(User(user_id=id, name=name, email=email, device_id=device_id, phone=phone, city=city,state=state, country=country,gender=gender, created_at=created_at))
        rows = [asdict(user) for user in users]
        file_name = "users.csv"
        self.__upload_to_gcs(f"user_data/{datetime.now().strftime('%Y%m%d')}/{file_name}", rows)
        logger.info(f"User data generation completed...{date}")        