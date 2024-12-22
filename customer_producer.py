
import sys
from confluent_kafka import Producer
import json
import time
from faker import Faker
sys.path.append('path_to_common_module_directory')
from config_reader import read_config

fake = Faker()

# Generate a fake customer
def get_random_customer():
    fake.unique.clear()
    return {
        "CustomerID": fake.unique.random_int(1, 10000000),
        "Name": fake.name(),
        "address": fake.address().replace("\n", " "),
        "Email": fake.email(),
        "city": fake.city(),
        "country": fake.country(),
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_customer_data(topic, config):
    producer = Producer(config)
    print("Starting Customer Producer...")

    try:
        while True:
            customer = get_random_customer()
            producer.produce(topic, key="customer", value=json_serializer(customer))
            print(f"Sent Customer: {customer}")
            producer.flush()
            time.sleep(1)  # Pause to simulate streaming
    except KeyboardInterrupt:
        print("Customer Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "customers"
    produce_customer_data(topic, config)
