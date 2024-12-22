
import sys
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
sys.path.append('path_to_common_module_directory')
from config_reader import read_config

fake = Faker()

def generate_taxrate():
    fake.unique.clear()
    return {
        "TaxID": fake.unique.random_int(1, 10000000),
        "TaxName": random.choice(["VAT", "Service Tax", "Sales Tax"]),
        "Rate": round(random.uniform(1.0, 20.0), 2)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_taxrate_data(topic, config):
    producer = Producer(config)
    print("Starting TaxRate Producer...")

    try:
        while True:
            taxrate = generate_taxrate()
            producer.produce(topic, key="taxrate", value=json_serializer(taxrate))
            print(f"Sent TaxRate: {taxrate}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("TaxRate Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "taxrates"
    produce_taxrate_data(topic, config)
