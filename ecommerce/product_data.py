from faker import Faker
from datetime import datetime
import random
from dataclasses import dataclass,asdict
from uuid import uuid4
import os
import csv
from google.cloud import storage
import io
from ecommerce.logger import logger
from ecommerce.gcs_to_local_download import DownloadFile

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

class ProductDataGenerator:
    def __init__(self, bucket_name: str = "gcs-ecommerce-data"):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()

    def __upload_to_gcs(self, destination_blob_name: str,rows):
        """ Uploads a file to the GCS bucket """
        logger.info(f"Product data upload to GCS started...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(destination_blob_name)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        logger.info(f"File {destination_blob_name} uploaded to {self.bucket_name}.")
        logger.info(f"Product data upload to GCS completed.")
        output.close()

    def generate_product(self, num_of_records: int):

        """ Generate product data bases on the number of records """
        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Starting product data generation...{date}")
        # Ensure supplier data is available
        supplier_file_name = f'gs://{self.bucket_name}/supplier_data/{datetime.now().strftime("%Y%m%d")}/suppliers.csv'
        download_file = DownloadFile()
        logger.info(f"Downloading supplier data from {supplier_file_name}...")
        supplier_file_path = download_file.download_from_gcs(gcs_path=supplier_file_name)
        with open(supplier_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            suppliers = [Supplier(**row) for row in reader]
        
        if not suppliers:
            logger.info("Supplier data not found. Please generate supplier data first.")
            return False

         # Generate product data

        products = []
        for _ in range(num_of_records):
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
            supplier_id = random.choice(suppliers).supplier_id
            quantity = random.randint(0, 100)
            if quantity == 0:
                product_availability = "Out of Stock"
            else:
                product_availability = "In Stock"
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            products.append(Product(product_id=id, name=full_name, category=category,supplier_id=supplier_id, price=price,quantity=quantity, availability=product_availability, created_at=created_at))
                # print(products)
        rows=[]
        for row in products:
            rows.append(asdict(row))
        # print(f"*****************{rows}")
        file_name = "products.csv"        
        self.__upload_to_gcs(f'product_data/{datetime.now().strftime("%Y%m%d")}/{file_name}',rows)

        logger.info(f"Product data generation completed...{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return products         
            # self.__upload_to_gcs('product_data/products.csv',rows)