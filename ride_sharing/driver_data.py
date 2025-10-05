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
class Vehical:
    vehicle_id: str
    make: str
    model: str
    year: int
    color: str
    license_plate: str
    vehicle_type: str
    created_at: datetime

class DriverDataGenerator:
    """ Class to generate user data and upload to GCS """

    def __init__(self, bucket_name: str = "gcs-ride-sharing-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    
    def __upload_to_gcs(self, destination_blob_name: str, rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Driver data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Driver data upload to GCS completed.")
        output.close()
    
    def generate_driver(self, num_of_records: int):
        """ Generate user data based on the number of records """
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting driver data generation...{date}")

        download_file = DownloadFile()
        vehical_file_name = f'gs://{self.bucket_name}/vehical_data/{datetime.now().strftime("%Y%m%d")}/vehicals.csv'
        vehical_local_path = download_file.download_from_gcs(gcs_path=vehical_file_name)

        with open(vehical_local_path, mode='r') as file:
            reader = csv.DictReader(file)
            vehicals = [Vehical(**row) for row in reader]

        drivers = []
        for _ in range(num_of_records):
            driver_id = "DRIVER-" + str(uuid4())[:8]
            name = fake.name()
            email = name.split(" ")[0].lower() + "_" + str(uuid4())[:8] + "@rideShare.com"
            phone = fake.phone_number()
            state = random.choice(list(state_city_map.keys()))
            city = random.choice(state_city_map[state])
            country = "India"
            id_proof = random.choice(["Aadhar Card", "PAN Card", "Voter ID", "Driving License"])
            license_number = state[:2].upper()+''.join(random.choices('0123456789', k=2)) + ''.join(random.choices('0123456789', k=11))
            vehicle_id = random.choice(vehicals).vehicle_id
            # Generate id_proof_number based on id_proof type
            if id_proof == "Aadhar Card":
                id_proof_number = fake.aadhaar_id()
            elif id_proof == "PAN Card":
                id_proof_number = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))+"P"+random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') + ''.join(random.choices('0123456789', k=4)) + random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            elif id_proof == "Voter ID":
                id_proof_number = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3)) + ''.join(random.choices('0123456789', k=7))
            else:  # Driving License
                id_proof_number = state[:2].upper()+''.join(random.choices('0123456789', k=2)) + ''.join(random.choices('0123456789', k=11))
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            driver = Driver(
                driver_id=driver_id,
                name=name,
                email=email,
                phone=phone,
                city= city,
                state=state,
                country=country,
                id_proof=id_proof,
                license_number=license_number,
                vehicle_id=vehicle_id,
                id_proof_number=id_proof_number,
                created_at=created_at
            )
            drivers.append(asdict(driver))
        
        file_name = "drivers.csv"
        self.__upload_to_gcs(f"driver_data/{datetime.now().strftime('%Y%m%d')}/{file_name}", drivers)
        logger.info(f"Driver data generation completed. Generated {num_of_records} records.")