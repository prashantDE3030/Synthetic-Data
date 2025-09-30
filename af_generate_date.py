from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ecommerce.ecommerce_data import EcommerceDataGenerator
from google.cloud import storage
import logging
from google.oauth2 import service_account
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def check_gcs_file_exists(gcs_path):
    parts = gcs_path.replace("gs://","").split('/')
    bucket_name, blob_name = parts[0],parts[1]
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    logging.info("Blob exists: {} and {}".format(blob,blob.exists()))
    return blob.exists()
def generate_dates(**kwargs):
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Start data generation...{date}")
    product_paths = 'gs://gcs-ecommerce-data/product_data/product_data/products.csv'
    customer_paths = 'gs://gcs-ecommerce-data/customer_data/customers.csv'
    if not check_gcs_file_exists(product_paths):
        EcommerceDataGenerator().generate_product(1000000)
    if not check_gcs_file_exists(customer_paths):
        EcommerceDataGenerator().generate_customer(1000000)
    EcommerceDataGenerator().generate_order(2000000,"gs://gcs-ecommerce-data/customer_data/customers.csv", "gs://gcs-ecommerce-data/product_data/products.csv")
    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"End data generation...{date}") 
    return True

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag= DAG(
    'ecommerce_data_generation',
    default_args=default_args,
    description='A DAG to generate synthetic ecommerce data',
    schedule_interval=timedelta(days=1),
)

generate_data_task = PythonOperator(
    task_id='generate_ecommerce_data',
    python_callable=generate_dates,
    dag=dag,
    )

generate_data_task