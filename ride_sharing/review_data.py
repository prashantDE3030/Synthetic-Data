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

fake = Faker("en_IN")

@dataclass
class Review:
    review_id: str
    driver_id: str
    user_id: str
    rating: int
    comment: str
    review_date: datetime

class ReviewDataGenerator:
    """Generates synthetic review data and uploads it to Google Cloud Storage."""
    def __init__(self, bucket_name: str = "gcs-ride-sharing-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
    
    def __upload_to_gcs(self, destination_blob_name: str, rows):
        """Uploads a file to the GCS bucket."""
        logger.info(f"Review data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Review data upload to GCS completed.")
        output.close()
    
    def generate_reviews(self, num_of_records: int):
        """Generates review data based on the number of records."""
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting review data generation...{date}")

        reviews = []
        for _ in range(num_of_records):
            review_id = "REVIEW-" + str(uuid4())[:8]
            driver_id = "DRIVER-" + str(uuid4())[:8]
            user_id = "USER-" + str(uuid4())[:8]
            rating = random.randint(1, 5)
            comment = fake.sentence(nb_words=20)
            review_date = fake.date_time_between(start_date='-1y', end_date='now').strftime("%Y-%m-%d %H:%M:%S")

            review = Review(
                review_id=review_id,
                driver_id=driver_id,
                user_id=user_id,
                rating=rating,
                comment=comment,
                review_date=review_date
            )
            reviews.append(asdict(review))
        file_name = 'reviews.csv'
        
        self.__upload_to_gcs(f'review_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',reviews)
        logger.info(f"Review data generation completed. Generated {num_of_records} records.")