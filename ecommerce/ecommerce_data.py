from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
import os
import csv
from google.cloud import storage
import io
import logging

fake = Faker("en_IN")

CATEGORIES = [
    "Electronics", "Clothing & Fashion", "Home & Garden", "Sports & Outdoors",
    "Books", "Health & Beauty", "Toys & Games", "Automotive", "Food & Beverages",
    "Office Supplies", "Pet Supplies", "Baby & Kids", "Jewelry & Watches"
]

PRODUCT_NAMES = {
    "Electronics": [
        "Smartphone", "Laptop", "Desktop Computer", "Tablet", "Smartwatch", "Headphones", "Earbuds",
        "Smart TV", "Gaming Console", "Smart Speaker", "Digital Camera", "Webcam", "Router", "Monitor",
        "Keyboard", "Mouse", "Power Bank", "Charger", "USB Cable", "Hard Drive", "SSD", "Memory Card",
        "Drone", "VR Headset", "Smart Home Hub", "Security Camera", "Bluetooth Speaker", "Printer",
        "Scanner", "Graphics Card", "Motherboard", "RAM", "CPU", "Smart Thermostat", "Smart Doorbell"
    ],
    "Clothing & Fashion": [
        "T-Shirt", "Polo Shirt", "Dress Shirt", "Blouse", "Tank Top", "Hoodie", "Sweater", "Cardigan",
        "Jeans", "Chinos", "Dress Pants", "Shorts", "Skirt", "Dress", "Jumpsuit", "Blazer", "Suit",
        "Jacket", "Coat", "Raincoat", "Sneakers", "Dress Shoes", "Boots", "Sandals", "Heels", "Flats",
        "Hat", "Cap", "Beanie", "Scarf", "Gloves", "Belt", "Tie", "Bow Tie", "Socks", "Underwear",
        "Bra", "Swimsuit", "Bikini", "Pajamas", "Robe", "Leggings", "Tights", "Purse", "Backpack",
        "Wallet", "Sunglasses", "Watch", "Jewelry", "Earrings", "Necklace", "Bracelet", "Ring"
    ],
    "Home & Garden": [
        "Coffee Maker", "Blender", "Toaster", "Microwave", "Refrigerator", "Dishwasher", "Washing Machine",
        "Dryer", "Vacuum Cleaner", "Air Purifier", "Humidifier", "Space Heater", "Fan", "Air Conditioner",
        "Sofa", "Chair", "Table", "Bed", "Mattress", "Pillow", "Bedsheet Set", "Comforter", "Blanket",
        "Lamp", "Ceiling Light", "Floor Lamp", "Desk Lamp", "Curtains", "Blinds", "Rug", "Carpet",
        "Mirror", "Picture Frame", "Wall Art", "Clock", "Vase", "Plant Pot", "Garden Hose", "Lawn Mower",
        "Trimmer", "Shovel", "Rake", "Gardening Gloves", "Seeds", "Fertilizer", "Watering Can", "Planter",
        "Outdoor Furniture", "Patio Set", "Grill", "Fire Pit", "Garden Statue", "Solar Lights"
    ],
    "Sports & Outdoors": [
        "Running Shoes", "Hiking Boots", "Athletic Shorts", "Sports Bra", "Yoga Mat", "Dumbbell Set",
        "Resistance Bands", "Treadmill", "Exercise Bike", "Elliptical", "Weight Bench", "Pull-up Bar",
        "Foam Roller", "Jump Rope", "Basketball", "Football", "Soccer Ball", "Tennis Racket", "Golf Clubs",
        "Baseball Bat", "Hockey Stick", "Skateboard", "Bicycle", "Mountain Bike", "Helmet", "Knee Pads",
        "Camping Tent", "Sleeping Bag", "Backpack", "Hiking Poles", "Compass", "GPS Device", "Headlamp",
        "Camping Stove", "Cooler", "Water Bottle", "Thermos", "First Aid Kit", "Multi-tool", "Rope",
        "Kayak", "Paddle", "Life Jacket", "Fishing Rod", "Tackle Box", "Hunting Knife", "Binoculars"
    ],
    "Books": [
        "Novel", "Biography", "Autobiography", "History Book", "Science Book", "Math Textbook",
        "Philosophy Book", "Psychology Book", "Self-Help Book", "Cookbook", "Travel Guide", "Dictionary",
        "Encyclopedia", "Atlas", "Poetry Book", "Short Stories", "Mystery Novel", "Romance Novel",
        "Science Fiction", "Fantasy Novel", "Horror Book", "Thriller", "Children's Book", "Picture Book",
        "Comic Book", "Graphic Novel", "Manga", "Art Book", "Photography Book", "Music Book",
        "Language Learning", "Study Guide", "Test Prep Book", "Business Book", "Finance Book",
        "Health Book", "Fitness Guide", "Parenting Book", "Religious Book", "Spiritual Guide"
    ],
    "Health & Beauty": [
        "Shampoo", "Conditioner", "Body Wash", "Soap", "Lotion", "Moisturizer", "Sunscreen", "Deodorant",
        "Perfume", "Cologne", "Makeup", "Foundation", "Concealer", "Mascara", "Lipstick", "Eye Shadow",
        "Blush", "Nail Polish", "Skincare Serum", "Face Mask", "Cleansing Oil", "Toner", "Exfoliator",
        "Anti-aging Cream", "Acne Treatment", "Hair Oil", "Hair Mask", "Styling Gel", "Hair Spray",
        "Brush", "Comb", "Hair Dryer", "Curling Iron", "Straightener", "Electric Razor", "Manual Razor",
        "Toothbrush", "Toothpaste", "Mouthwash", "Dental Floss", "Vitamins", "Supplements", "Protein Powder",
        "Essential Oils", "Diffuser", "Massage Oil", "Bath Bombs", "Epsom Salt", "Tweezers", "Nail Clippers"
    ],
    "Toys & Games": [
        "Action Figure", "Doll", "Stuffed Animal", "Building Blocks", "LEGO Set", "Puzzle", "Board Game",
        "Card Game", "Video Game", "Gaming Controller", "Remote Control Car", "Drone Toy", "Robot Toy",
        "Educational Toy", "Musical Instrument", "Art Supplies", "Coloring Book", "Crayons", "Markers",
        "Play-Doh", "Slime Kit", "Science Kit", "Magic Set", "Costume", "Dress-up Clothes", "Toy Kitchen",
        "Tool Set", "Doctor Kit", "Cash Register", "Toy Car", "Train Set", "Dollhouse", "Ball", "Frisbee",
        "Jump Rope", "Hula Hoop", "Kite", "Bubbles", "Sidewalk Chalk", "Toy Guns", "Water Gun", "Nerf Blaster",
        "Trading Cards", "Collectible Figures", "Model Kit", "Remote Control Helicopter", "Yo-Yo"
    ],
    "Automotive": [
        "Car Battery", "Engine Oil", "Brake Pads", "Air Filter", "Spark Plugs", "Tire", "Wheel",
        "Car Cover", "Floor Mats", "Seat Covers", "Steering Wheel Cover", "Car Charger", "Phone Mount",
        "Dash Cam", "GPS Navigator", "Car Vacuum", "Jumper Cables", "Tool Kit", "Emergency Kit",
        "Windshield Wipers", "Headlight Bulb", "Tail Light", "Car Wax", "Car Wash Soap", "Microfiber Cloth",
        "Tire Pressure Gauge", "Jack", "Lug Wrench", "Funnel", "Coolant", "Transmission Fluid",
        "Power Steering Fluid", "Brake Fluid", "Car Polish", "Leather Conditioner", "Air Freshener",
        "Sunshade", "Cargo Net", "Roof Rack", "Bike Rack", "Trailer Hitch", "Towing Strap", "Fuses"
    ],
    "Food & Beverages": [
        "Coffee Beans", "Ground Coffee", "Tea Bags", "Loose Leaf Tea", "Energy Drink", "Sports Drink",
        "Juice", "Soda", "Water Bottles", "Wine", "Beer", "Spirits", "Protein Bar", "Granola Bar",
        "Cereal", "Oatmeal", "Bread", "Pasta", "Rice", "Quinoa", "Nuts", "Dried Fruit", "Snack Mix",
        "Chips", "Crackers", "Cookies", "Chocolate", "Candy", "Gum", "Honey", "Jam", "Peanut Butter",
        "Olive Oil", "Coconut Oil", "Vinegar", "Salt", "Pepper", "Spices", "Herbs", "Hot Sauce",
        "Ketchup", "Mustard", "Mayo", "Salad Dressing", "Canned Soup", "Frozen Meals", "Ice Cream",
        "Yogurt", "Cheese", "Milk", "Eggs", "Butter", "Flour", "Sugar", "Baking Powder", "Vanilla Extract"
    ],
    "Office Supplies": [
        "Pen", "Pencil", "Marker", "Highlighter", "Eraser", "Ruler", "Stapler", "Paper Clips", "Binder Clips",
        "Rubber Bands", "Push Pins", "Tape", "Glue", "Scissors", "Paper Shredder", "Calculator", "Notebook",
        "Sticky Notes", "Index Cards", "File Folders", "Binders", "Sheet Protectors", "Label Maker",
        "Labels", "Stamps", "Ink Pad", "Envelopes", "Paper", "Cardstock", "Laminator", "Laminating Pouches",
        "Desk Organizer", "Pencil Holder", "Paper Tray", "Bulletin Board", "Whiteboard", "Dry Erase Markers",
        "Calendar", "Planner", "Address Book", "Business Cards", "Name Tags", "Lanyard", "Badge Holder",
        "Presentation Folder", "Portfolio", "Clipboard", "Hole Punch", "Book Ends", "Desk Pad", "Mouse Pad"
    ],
    "Pet Supplies": [
        "Dog Food", "Cat Food", "Bird Food", "Fish Food", "Dog Treats", "Cat Treats", "Pet Bowl",
        "Water Dispenser", "Dog Leash", "Cat Collar", "Pet Harness", "Pet Carrier", "Dog Bed", "Cat Bed",
        "Pet Blanket", "Scratching Post", "Cat Litter", "Litter Box", "Pet Shampoo", "Pet Brush",
        "Nail Clippers", "Pet Toothbrush", "Dental Chews", "Flea Treatment", "Pet Medicine", "Vitamins",
        "Dog Toy", "Cat Toy", "Ball", "Rope Toy", "Squeaky Toy", "Catnip", "Pet Gate", "Dog Crate",
        "Exercise Pen", "Pet Stroller", "Car Seat", "Seat Cover", "Pet Ramp", "Training Pads",
        "Waste Bags", "Pooper Scooper", "Aquarium", "Fish Tank", "Water Filter", "Aquarium Heater",
        "Decorations", "Gravel", "Plants", "Bird Cage", "Perch", "Nest Box", "Hamster Wheel"
    ],
    "Baby & Kids": [
        "Baby Formula", "Baby Food", "Diapers", "Baby Wipes", "Diaper Rash Cream", "Baby Lotion",
        "Baby Shampoo", "Baby Powder", "Pacifier", "Baby Bottle", "Sippy Cup", "High Chair", "Booster Seat",
        "Car Seat", "Stroller", "Baby Carrier", "Crib", "Changing Table", "Baby Monitor", "Night Light",
        "Mobile", "Play Mat", "Bouncer", "Swing", "Walker", "Exersaucer", "Baby Gate", "Outlet Covers",
        "Cabinet Locks", "Teething Toys", "Rattles", "Soft Toys", "Books", "Bath Toys", "Potty Seat",
        "Training Pants", "Bibs", "Burp Cloths", "Swaddle Blankets", "Sleep Sack", "Onesies", "Pajamas",
        "Socks", "Shoes", "Hat", "Mittens", "Snowsuit", "Backpack", "Lunch Box", "Thermos", "Bento Box"
    ],
    "Jewelry & Watches": [
        "Engagement Ring", "Wedding Ring", "Diamond Ring", "Gold Ring", "Silver Ring", "Fashion Ring",
        "Earrings", "Stud Earrings", "Hoop Earrings", "Drop Earrings", "Chandelier Earrings", "Necklace",
        "Chain Necklace", "Pendant Necklace", "Choker", "Statement Necklace", "Bracelet", "Chain Bracelet",
        "Charm Bracelet", "Bangle", "Cuff Bracelet", "Tennis Bracelet", "Anklet", "Brooch", "Cufflinks",
        "Tie Clip", "Watch", "Smart Watch", "Digital Watch", "Analog Watch", "Luxury Watch", "Sports Watch",
        "Dress Watch", "Diving Watch", "Chronograph", "Watch Band", "Watch Strap", "Jewelry Box",
        "Ring Holder", "Necklace Stand", "Earring Organizer", "Travel Jewelry Case", "Cleaning Cloth",
        "Jewelry Cleaner", "Ring Sizer", "Magnifying Glass", "Gemstone", "Pearl", "Diamond", "Gold Chain"
    ]
}
#BRANDS = ["Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Amazon", "Google", "Microsoft", "Dell", "HP", "Canon", "Nikon"]

@dataclass
class Product:
    product_id: str
    name: str
    category: str
    price: float
    quantity: int
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
class Order:
    order_id: str
    customer_id: str
    product_id: str
    payment_id: str
    total_price: float
    order_date: datetime

@dataclass
class PaymentMethod:
    payment_id: str
    payment_method: str
    payment_date: datetime
    status: str

@dataclass
class Return:
    return_id: str
    order_id: str
    product_id: str
    customer_id: str
    reason: str
    return_date: datetime
    status: str

@dataclass
class Cancelation:
    cancelation_id: str
    order_id: str
    product_id: str
    customer_id: str
    reason: str
    cancelation_date: datetime
    status: str

@dataclass
class Review:
    review_id: str
    product_id: str
    customer_id: str
    rating: int
    comment: str
    review_date: datetime

@dataclass
class Shipment:
    shipment_id: str
    order_id: str
    shipment_date: datetime
    delivery_date: datetime
    status: str
    tracking_number: str

@dataclass
class Supplier:
    supplier_id: str
    name: str
    contact_name: str
    address: str
    phone: str
    email: str
    city: str
    country: str
    created_at: datetime

@dataclass
class Device:
    device_id: str
    customer_id: str
    device_type: str
    os: str
    browser: str
    ip_address: str
    last_login: datetime

@dataclass
class Cart:
    cart_id: str
    customer_id: str
    product_id: str
    quantity: int
    added_at: datetime

@dataclass
class ProductView:
    view_id: str
    customer_id: str
    product_id: str
    view_date: datetime


class EcommerceDataGenerator:
    def __init__(self,seed: int = 42):
        random.seed(seed)
        fake.seed_instance(seed)
        self.storage_client = storage.Client()
        self.bucket_name = "gcs-ecommerce-data"  # Replace with your GCS bucket name
    
    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logging.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
    
    def generate_payment(self, num_of_records: int):
        """ Generate payment data bases on the number of records """
        payments = []
        for i in range(num_of_records):
            id = "PAY-" + str(uuid4())[:8]
            payment_method = random.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash on Delivery"])
            payment_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = random.choice(["Completed", "Pending", "Failed", "Refunded"]) if random.random() < 0.9 else "Cash on Delivery"
            payments.append(PaymentMethod(payment_id=id,payment_method=payment_method, payment_date=payment_date, status=status))
        rows=[]
        for row in payments:
            rows.append(asdict(row))
        file_name = "payments.csv"        
        self.__upload_to_gcs(f'payment_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('payment_data/payments.csv',rows)
        #return payments
    
    def generate_shipment(self, num_of_records: int):
        """ Generate shipment data bases on the number of records """
        shipments = []
        for i in range(num_of_records):
            id = "SHIP-" + str(uuid4())[:8]
            order_id = "ORD-" + str(uuid4())[:8]
            shipment_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            delivery_date = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
            status = random.choice(["Shipped", "In Transit", "Delivered", "Delayed", "Cancelled"])
            tracking_number = str(uuid4()).replace("-", "").upper()[:12]
            shipments.append(Shipment(shipment_id=id, order_id=order_id, shipment_date=shipment_date, delivery_date=delivery_date, status=status, tracking_number=tracking_number))
        rows=[]
        for row in shipments:
            rows.append(asdict(row))
        file_name = "shipments.csv"        
        self.__upload_to_gcs(f'shipment_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('shipment_data/shipments.csv',rows)
        #return shipments
    
    def generate_cancelation(self, num_of_records: int):
        """ Generate cancelation data bases on the number of records """
        cancelations = []
        for i in range(num_of_records):
            id = "CAN-" + str(uuid4())[:8]
            order_id = "ORD-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            reason = random.choice(["Changed Mind", "Found Better Price", "Shipping Delay", "Ordered by Mistake", "Other"])
            cancelation_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "Cancelled"
            cancelations.append(Cancelation(cancelation_id=id, order_id=order_id, product_id=product_id, customer_id=customer_id, reason=reason, cancelation_date=cancelation_date, status=status))
        rows=[]
        for row in cancelations:
            rows.append(asdict(row))
        file_name = "cancelations.csv"        
        self.__upload_to_gcs(f'cancelation_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('cancelation_data/cancelations.csv',rows)
        #return cancelations
    
    def generate_review(self, num_of_records: int):
        """ Generate review data bases on the number of records """
        reviews = []
        for i in range(num_of_records):
            id = "REV-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            rating = random.randint(1, 5)
            comment = fake.sentence(nb_words=10)
            review_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            reviews.append(Review(review_id=id, product_id=product_id, customer_id=customer_id, rating=rating, comment=comment, review_date=review_date))
        rows=[]
        for row in reviews:
            rows.append(asdict(row))
        file_name = "reviews.csv"        
        self.__upload_to_gcs(f'review_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('review_data/reviews.csv',rows)
        #return reviews

    def generate_supplier(self, num_of_records: int):
        """ Generate supplier data bases on the number of records """
        suppliers = []
        for i in range(num_of_records):
            id = "SUP-" + str(uuid4())[:8]
            name = fake.company()
            contact_name = fake.name()
            address = fake.address().replace("\n", ", ")
            phone = fake.phone_number()
            email = contact_name.split(" ")[0].lower()+"_"+str(uuid4())[:8]+"@supplier.com"
            city = fake.city()
            country = "India"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            suppliers.append(Supplier(supplier_id=id, name=name, contact_name=contact_name, address=address, phone=phone, email=email, city=city, country=country, created_at=created_at))
        rows=[]
        for row in suppliers:
            rows.append(asdict(row))
        file_name = "suppliers.csv"        
        self.__upload_to_gcs(f'supplier_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
    
    def generate_return(self, num_of_records: int):
        """ Generate return data bases on the number of records """
        returns = []
        for i in range(num_of_records):
            id = "RET-" + str(uuid4())[:8]
            order_id = "ORD-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            reason = random.choice(["Defective", "Wrong Item", "Changed Mind", "Better Price Elsewhere", "No Longer Needed"])
            return_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = "Returned"
            returns.append(Return(return_id=id, order_id=order_id, product_id=product_id, customer_id=customer_id, reason=reason, return_date=return_date, status=status))
        rows=[]
        for row in returns:
            rows.append(asdict(row))
        file_name = "returns.csv"        
        self.__upload_to_gcs(f'return_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)

    
    def generate_device(self, num_of_records: int):
        """ Generate device data bases on the number of records """
        devices = []
        for i in range(num_of_records):
            id = "DEV-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            device_type = random.choice(["Mobile", "Tablet", "Desktop", "Laptop"])
            os = random.choice(["iOS", "Android", "Windows", "macOS", "Linux"])
            browser = random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"])
            ip_address = fake.ipv4()
            last_login = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            devices.append(Device(device_id=id, customer_id=customer_id, device_type=device_type, os=os, browser=browser, ip_address=ip_address, last_login=last_login))
        rows=[]
        for row in devices:
            rows.append(asdict(row))
        file_name = "devices.csv"        
        self.__upload_to_gcs(f'device_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('device_data/devices.csv',rows)
        #return devices
    def generate_cart(self, num_of_records: int):
        """ Generate cart data bases on the number of records """
        carts = []
        for i in range(num_of_records):
            id = "CART-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            quantity = random.randint(1, 5)
            added_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            carts.append(Cart(cart_id=id, customer_id=customer_id, product_id=product_id, quantity=quantity, added_at=added_at))
        rows=[]
        for row in carts:
            rows.append(asdict(row))
        file_name = "carts.csv"        
        self.__upload_to_gcs(f'cart_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('cart_data/carts.csv',rows)
        #return carts
    
    def generate_product_view(self, num_of_records: int):
        """ Generate product view data bases on the number of records """
        product_views = []
        for i in range(num_of_records):
            id = "VIEW-" + str(uuid4())[:8]
            customer_id = "CUST-" + str(uuid4())[:8]
            product_id = "PROD-" + str(uuid4())[:8]
            view_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            product_views.append(ProductView(view_id=id, customer_id=customer_id, product_id=product_id, view_date=view_date))
        rows=[]
        for row in product_views:
            rows.append(asdict(row))
        file_name = "product_views.csv"        
        self.__upload_to_gcs(f'product_view_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('product_view_data/product_views.csv',rows)
        #return product_views


    def generate_product(self, num_of_records: int):

        """ Generate product data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Starting product data generation...{date}")    
        products = []
        for i in range(num_of_records):
            id = "PROD-" + str(uuid4())[:8]
            category = random.choice(CATEGORIES)
            name = random.choice(PRODUCT_NAMES[category])
           # brand = random.choice(BRANDS)
            if category == "Electronics":
                price = round(random.uniform(1000.0, 200000.0), 2)
            elif category == "Clothing & Fashion":
                price = round(random.uniform(200.0, 50000.0), 2)
            elif category == "Home & Garden":
                price = round(random.uniform(150.0, 1800.0), 2)
            elif category == "Sports & Outdoors":
                price = round(random.uniform(250.0, 60000.0), 2)
            else:
                price = round(random.uniform(125.0, 6000.0), 2)
            full_name = name
            quantity = random.randint(0, 100)
            if quantity == 0:
                product_availability = "Out of Stock"
            else:
                product_availability = "In Stock"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            products.append(Product(product_id=id, name=full_name, category=category, price=price,quantity=quantity, availability=product_availability, created_at=created_at))
            # print(products)
        rows=[]
        for row in products:
            rows.append(asdict(row))
        # print(f"*****************{rows}")
        file_name = "products.csv"        
        self.__upload_to_gcs(f'product_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)         
        # self.__upload_to_gcs('product_data/products.csv',rows)
        
    def generate_customer(self, num_of_records: int):

        """ Generate customer data bases on the number of records """

        customers = []
        for i in range(num_of_records):
            id = "CUST-" + str(uuid4())[:8]
            name = fake.name()
            email = name.split(" ")[0].lower()+"_"+str(uuid4())[:8]+"@namasteKart.com"
            gender = random.choice(["Male","Female","Other"])
            address = fake.address().replace("\n", ", ")
            phone = fake.phone_number()
            city = fake.city()
            country = "India"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            customers.append(Customer(customer_id=id, name=name, email=email,gender=gender, address=address, phone=phone, city=city, country=country, created_at=created_at))
        rows=[]
        for row in customers:
            rows.append(asdict(row))
        file_name = "customers.csv"        
        self.__upload_to_gcs(f'customer_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)        
        # self.__upload_to_gcs('customer_data/customers.csv',rows)
        #return customers
# Ex
    def __download_from_gcs(self, gcs_path,local_path="tmp"):
        """ Downloads a file from the GCS bucket """
        parts = gcs_path.replace("gs://", "").split("/", 1)
        bucket_name = parts[0]
        source_blob_name = parts[1]
        print(f"GCS Bucket: {bucket_name}, Blob: {source_blob_name}")
        local_path = os.path.join(local_path, os.path.basename(source_blob_name))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"Local path for download: {local_path}")
        bucket = self.storage_client.bucket(bucket_name)
        print(f"Accessing bucket: {bucket.name}")
        blob = bucket.blob(source_blob_name)
        print(f"Downloading blob: {blob.name}")
        blob.download_to_filename(local_path)
        print(f"File {source_blob_name} downloaded from bucket {bucket_name} to {local_path}.")
        return local_path

    def generate_order(self, num_of_records: int, customer_file_name, products_file_name):

        """ Generate order data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Start order data generation...{date}") 
        orders = []
        customer_file_name = self.__download_from_gcs(customer_file_name)
        products_file_name = self.__download_from_gcs(products_file_name)
        with open(customer_file_name, mode='r') as file:
            reader = csv.DictReader(file)
            customers = [Customer(**row) for row in reader]
        with open(products_file_name, mode='r') as file:
            reader = csv.DictReader(file)
            products = [Product(**row) for row in reader]
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
            payment_id = "PAY-" + str(uuid4())[:8]
            # print(product)
            quantity = product.quantity
            # print(f"Quantity ***************************> {quantity}")
            total_price = round(float(product.price) * float(quantity), 2)
            order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders.append(Order(order_id=id, customer_id=customer.customer_id, product_id=product.product_id,payment_id=payment_id, total_price=total_price, order_date=order_date))
        rows=[]
        for row in orders:
            rows.append(asdict(row))
        file_name = "orders.csv"        
        self.__upload_to_gcs(f'order_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)
        return True
# product_data = EcommerceDataGenerator().generate_product(1000000)
# customer_data = EcommerceDataGenerator().generate_customer(1000000)
# order_data = EcommerceDataGenerator().generate_order(100000000,"customer_data/customers.csv", "product_data/products.csv")
# date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# print(f"End data generation...{date}") 
# row=[]
# for order in order_data:
#     row.append(asdict(order))
# print(row)
# print(row[0].keys())


# # print(fake.name())  # Generates a random name
# # fake.address()  # Generates a random address
# # fake.email()  # Generates a random email

# print(fake.city())  # Generates a random city name
# print(fake.country())  # Generates a random country name

def generate_all_data():
    EcommerceDataGenerator().generate_product(10000)
    EcommerceDataGenerator().generate_customer(10000)
    EcommerceDataGenerator().generate_payment(10000)
    EcommerceDataGenerator().generate_order(20000,f'gs://gcs-ecommerce-data/customer_data/{datetime.now().strftime("%Y%m%d")}/customers.csv', f'gs://gcs-ecommerce-data/product_data/{datetime.now().strftime("%Y%m%d")}/products.csv')
    EcommerceDataGenerator().generate_shipment(2000)
    EcommerceDataGenerator().generate_cancelation(100)
    EcommerceDataGenerator().generate_review(15000)
    EcommerceDataGenerator().generate_supplier(2000)
    EcommerceDataGenerator().generate_return(1500)
    EcommerceDataGenerator().generate_device(20000)
    EcommerceDataGenerator().generate_cart(1000)
    EcommerceDataGenerator().generate_product_view(12000)

generate_all_data()

