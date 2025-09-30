from ecommerce.product_data import ProductDataGenerator
from ecommerce.cancelation_data import CancelationDataGenerator
from ecommerce.orders_data import OrderDataGenerator
from ecommerce.customer_data import CustomerDataGenerator
from ecommerce.supplier_data import SupplierDataGenerator
from ecommerce.cart_data import CartDataGenerator
from ecommerce.review_data import ReviewDataGenerator
from ecommerce.device_data import DeviceDataGenerator
from ecommerce.payment_data import PaymentDataGenerator
from ecommerce.product_view_data import ProductViewDataGenerator
from ecommerce.return_data import ReturnDataGenerator
from ecommerce.shipment_data import ShipmentDataGenerator

def main():
    num_of_records = 100000  # Specify the number of records you want to generate

    # Generate and upload supplier data
    supplier_generator = SupplierDataGenerator()
    supplier_generator.generate_supplier(num_of_records)

    # Generate and upload product data
    product_generator = ProductDataGenerator()
    product_generator.generate_product(num_of_records)


    # Generate and upload customer data
    customer_generator = CustomerDataGenerator()
    customer_generator.generate_customer(num_of_records)

    # Generate and upload device data
    device_generator = DeviceDataGenerator()
    device_generator.generate_device(num_of_records)

    # Generate and upload payment data
    payment_generator = PaymentDataGenerator()
    payment_generator.generate_payment(200000)

    # Generate and upload order data
    order_generator = OrderDataGenerator()
    order_generator.generate_order(200000)

    # Generate and upload cart data
    cart_generator = CartDataGenerator()
    cart_generator.generate_cart(100)

    # Generate and upload review data
    review_generator = ReviewDataGenerator()
    review_generator.generate_reviews(10000)

    # Generate and upload product view data
    product_view_generator = ProductViewDataGenerator()
    product_view_generator.generate_product_views(1000)

    # Generate and upload return data
    return_generator = ReturnDataGenerator()
    return_generator.generate_return(1000)

    # Generate and upload shipment data
    shipment_generator = ShipmentDataGenerator()
    shipment_generator.generate_shipment(10000)

    # Generate and upload cancelation data
    cancelation_generator = CancelationDataGenerator()
    cancelation_generator.generate_cancelation(1000)

if __name__ == "__main__":
    main()