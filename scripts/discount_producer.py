
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'common')))
from config_reader import read_config

fake = Faker()

def generate_discount():
    return {
        "DiscountID": fake.random_int(1, 10),
        "DiscountName": fake.word(),
        "DiscountValue": round(random.uniform(5.0, 50.0), 2)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_discount_data(topic, config):
    producer = Producer(config)
    print("Starting Discount Producer...")

    try:
        while True:
            discount = generate_discount()
            producer.produce(topic, key="discount", value=json_serializer(discount))
            print(f"Sent Discount: {discount}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Discount Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "discounts"
    produce_discount_data(topic, config)
