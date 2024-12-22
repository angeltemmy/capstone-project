
import sys
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
sys.path.append('path_to_common_module_directory')
from config_reader import read_config

fake = Faker()

def generate_product():
    fake.unique.clear()
    return {
        "ProductID": fake.unique.random_int(1, 5000000),
        "ProductName": fake.word(),
        "Description": fake.text(max_nb_chars=100),
        "Price": round(random.uniform(5.0, 1000.0), 2),
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_product_data(topic, config):
    producer = Producer(config)
    print("Starting Product Producer...")

    try:
        while True:
            product = generate_product()
            producer.produce(topic, key="product", value=json_serializer(product))
            print(f"Sent Product: {product}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Product Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "products"
    produce_product_data(topic, config)
