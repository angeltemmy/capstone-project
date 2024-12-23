
import sys
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
sys.path.append('path_to_common_module_directory')
from config_reader import read_config
fake = Faker()
from datetime import datetime, timedelta


def generate_random_date_in_last_10_years():
    today = datetime.today()
    start_date = today - timedelta(days=365 * 10)  # 10 years ago
    random_date = start_date + timedelta(days=random.randint(0, 365 * 10))
    return random_date.isoformat()

def generate_shipping_details(invoice_id):
    fake.unique.clear()
    return {
        "ShippingID": fake.unique.random_int(1, 5000000),
        "InvoiceID": invoice_id,
        "Address": fake.address().replace("\n", " "),
        "ShippingDate": generate_random_date_in_last_10_years(),
        "EstimatedArrival": generate_random_date_in_last_10_years()
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_shipping_details_data(topic, config):
    producer = Producer(config)
    print("Starting Shipping Details Producer...")

    try:
        while True:
            invoice_id = fake.random_int(1, 2000)
            shipping_detail = generate_shipping_details(invoice_id)
            producer.produce(topic, key="shipping_detail", value=json_serializer(shipping_detail))
            print(f"Sent Shipping Detail: {shipping_detail}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shipping Details Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "shipping_details"
    produce_shipping_details_data(topic, config)
