
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

def generate_payment_method():
    fake.unique.clear()
    return {
        "MethodID": fake.unique.random_int(1, 100000),
        "MethodName": random.choice(["Credit Card", "PayPal", "Bank Transfer", "Cash", "Cryptocurrency"]),
        "Description": fake.text(max_nb_chars=50)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_payment_method_data(topic, config):
    producer = Producer(config)
    print("Starting Payment Method Producer...")

    try:
        while True:
            payment_method = generate_payment_method()
            producer.produce(topic, key="payment_method", value=json_serializer(payment_method))
            print(f"Sent Payment Method: {payment_method}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Payment Method Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "payment_methods"
    produce_payment_method_data(topic, config)
