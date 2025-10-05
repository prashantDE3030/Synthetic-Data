from ride_sharing.device_data import DeviceDataGenerator
from ride_sharing.user_data import UserDataGenerator
from ride_sharing.vehical_data import VehicalDataGenerator
from ride_sharing.driver_data import DriverDataGenerator
from ride_sharing.location_data import LocationDataGenerator
from ride_sharing.payment_data import PaymentDataGenerator
from ride_sharing.promo_data import PromoDataGenerator
from ride_sharing.trip_data import TripDataGenerator
from ride_sharing.review_data import ReviewDataGenerator
import os
import glob
def main():
    num_of_records = 100000

    # Generate and upload device data
    device_generator = DeviceDataGenerator()
    device_generator.generate_device(num_of_records)

    # Generate and upload user data
    user_generator = UserDataGenerator()
    user_generator.generate_user(num_of_records)

    # Generate and upload vehical data
    vehical_generator = VehicalDataGenerator()
    vehical_generator.generate_vehical(num_of_records)

    # Generate and upload driver data
    driver_generator = DriverDataGenerator()
    driver_generator.generate_driver(num_of_records)

    # Generate and upload location data
    location_generator = LocationDataGenerator()
    location_generator.generate_location(num_of_records)

    # Generate and upload payment data
    payment_generator = PaymentDataGenerator()
    payment_generator.generate_payment(num_of_records)

    # Generate and upload promo_code data
    promo_generator = PromoDataGenerator()
    promo_generator.generate_promo(100)

    # Generate and upload trip data
    trip_generator = TripDataGenerator()
    trip_generator.generate_trip_data(200000)

    # Generate and upload review data
    review_generator = ReviewDataGenerator()
    review_generator.generate_reviews(100)

    for file in glob.glob(pathname=r"d:\Synthetic_Data\tmp\*.csv"):
        os.remove(file)



if __name__=="__main__":
    main()