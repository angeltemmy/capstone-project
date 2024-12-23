
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
def generate_invoice(customer_id):
    fake.unique.clear()
    return {
        "InvoiceID": fake.unique.random_int(1, 2000),
        "CustomerID": customer_id,
        "InvoiceDate": generate_random_date_in_last_10_years(),
        "DueDate": fake.date_this_year().isoformat(),
        "TotalAmount": round(random.uniform(100, 5000), 2)
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def produce_invoice_data(topic, config):
    producer = Producer(config)
    print("Starting Invoice Producer...")

    try:
        while True:
            customer_id = fake.random_int(1, 10000000)  # Assuming customer IDs are pre-generated
            invoice = generate_invoice(customer_id)
            producer.produce(topic, key="invoice", value=json_serializer(invoice))
            print(f"Sent Invoice: {invoice}")
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Invoice Producer stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    config = read_config("client.properties")
    topic = "invoices"
    produce_invoice_data(topic, config)
