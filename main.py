from ecommerce.ecommerce_data import EcommerceDataGenerator
import csv
import os

def main():
    generator = EcommerceDataGenerator()
    product_data = generator.generate_product(10000)
      # Generate 1000 records
    customer_data = generator.generate_customer(1000)

    order_data = generator.generate_order(5000, customer_data, product_data)

    # Write product data to product.csv file
