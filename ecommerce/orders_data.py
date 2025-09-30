from faker import Faker
from datetime import datetime,timedelta
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
class Order:
    order_id: str
    customer_id: str
    product_id: str
    payment_id: str
    total_price: float
    order_date: datetime
    expected_delivery_date: datetime

@dataclass
class Product:
    product_id: str
    name: str
    category: str
    price: float
    quantity: int
    supplier_id: str
    availability: str
    created_at: datetime

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

@dataclass
class PaymentMethod:
    payment_id: str
    payment_method: str
    payment_date: datetime
    status: str

class OrderDataGenerator:
    """Generates synthetic order data and uploads it to Google Cloud Storage."""
    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def __upload_to_gcs(self, destination_blob_name: str, rows):
        """Uploads a file to the GCS bucket."""
        logger.info(f"Order data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Order data upload to GCS completed.")
        output.close()
    
    def generate_order(self, num_of_records: int) -> bool:

        """ Generate order data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Start order data generation...{date}") 
        orders = []
        download_file = DownloadFile()
        customer_file_name = download_file.download_from_gcs(f'gs://{self.bucket_name}/customer_data/{datetime.now().strftime("%Y%m%d")}/customers.csv')
        products_file_name =  download_file.download_from_gcs(f'gs://{self.bucket_name}/product_data/{datetime.now().strftime("%Y%m%d")}/products.csv')
        payment_file_name =  download_file.download_from_gcs(f'gs://{self.bucket_name}/payment_data/{datetime.now().strftime("%Y%m%d")}/payments.csv')

    
        with open(customer_file_name, mode='r') as file:
            reader = csv.DictReader(file)
            customers = [Customer(**row) for row in reader]
        with open(products_file_name, mode='r') as file:
            reader = csv.DictReader(file)
            products = [Product(**row) for row in reader]
        with open(payment_file_name, mode='r') as file:
            reader = csv.DictReader(file)
            payments = [PaymentMethod(**row) for row in reader]
         # Filter out customers with None customer_id and products that are "In Stock"
        customers = [cust for cust in customers if cust.customer_id is not None]
        # print(f"Customer ***************************> {customers}")
        products = [prod for prod in products if prod.availability == "In Stock"]
        # print(f"Product ***************************> {products}")
        for i in range(num_of_records):
            id = "ORD-" + str(uuid4())[:8]
            customer = random.choice(customers)
            # print(customer)
            product = random.choice(products)
            payment_id = random.choice(payments).payment_id
            # print(product)
            quantity = product.quantity
            # print(f"Quantity ***************************> {quantity}")
            total_price = round(float(product.price) * float(quantity), 2)
            order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            random_days = random.randint(1, 30)
            expected_delivery_date = (datetime.now() + timedelta(random_days)).strftime("%Y-%m-%d %H:%M:%S")
            orders.append(Order(order_id=id, customer_id=customer.customer_id, product_id=product.product_id,payment_id=payment_id, total_price=total_price, order_date=order_date, expected_delivery_date=expected_delivery_date))
        rows=[]
        for row in orders:
            rows.append(asdict(row))
        file_name = "orders.csv"        
        self.__upload_to_gcs(f'order_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        return True