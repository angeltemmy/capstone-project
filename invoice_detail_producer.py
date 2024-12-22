
import sys
from confluent_kafka import Producer
import json
import time
import random
from faker import Faker
sys.path.append('path_to_common_module_directory')
from config_reader import read_config

fake = Faker()

def generate_invoice_detail(invoice_id, product_id, tax_id):
    quantity = random.randint(1, 10000)
    return {
        "InvoiceID": invoice_id,
        "ProductID": product_id,
        "Quantity": quantity,
        "TaxID": tax_id,
        "LineTotal": round(quantity * random.uniform(10, 100), 2)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_invoice_detail_data(topic, config):
    producer = Producer(config)
    print("Starting Invoice Detail Producer...")

    try:
        while True:
            invoice_id = fake.random_int(1, 2000)
            product_id = fake.random_int(1, 5000000)
            tax_id = fake.random_int(1, 10000000)
            invoice_detail = generate_invoice_detail(invoice_id, product_id, tax_id)
            producer.produce(topic, key="invoice_detail", value=json_serializer(invoice_detail))
            print(f"Sent Invoice Detail: {invoice_detail}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Invoice Detail Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "invoice_details"
    produce_invoice_detail_data(topic, config)
