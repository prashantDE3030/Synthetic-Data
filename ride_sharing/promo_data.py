from faker import Faker
from datetime import datetime, timedelta
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
# import os
import csv
from google.cloud import storage
import io
from ride_sharing.logger import logger
fake = Faker("en_IN")


@dataclass
class Promo:
    promo_id: str
    promo_code: str
    discount_type: str
    discount_value: float
    start_date: datetime
    end_date: datetime


class PromoDataGenerator:

    """ Class to generate promo code data and upload to GCS  """

    def __init__(self, bucket_name: str = "gcs-ride-sharing-data"):
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

    def generate_promo(self,num_of_records: int):

        """Generate Promo code data bases on the number of records"""

        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting promo code data generation...{date}")
        promos = []
        for _ in range(num_of_records):
            valid_days = random.randint(7,90)
            code_pattern = ["XXXX-XXXX", "PROMO-XXXX", "SAVE-XX", "X4X4X4"]
            is_percent = random.choice([True, False])
            promo_id = "PROM-"+ str(uuid4())[:8]
            promo_code = random.choice(code_pattern)
            discount_type = "Percent" if is_percent else "Amount"
            discount_value = random.randint(5,50) if discount_type == "Percent" else random.randint(50,200)
            start_date = fake.date_between(start_date= "-30d", end_date= "+10d")
            end_date = start_date + timedelta(days= valid_days)
            promos.append(Promo(promo_id=promo_id,promo_code=promo_code,discount_type=discount_type,discount_value=discount_value,start_date=start_date.strftime('%Y-%m-%d'),end_date=end_date.strftime('%Y-%m-%d')))
        rows=[]
        rows = [asdict(promo) for promo in promos]
        file_name = "promo_code.csv"
        self.__upload_to_gcs(f"promo_code/{datetime.now().strftime('%Y%m%d')}/{file_name}",rows)

        logger.info(f"Promo code data generation completed...{date}")




