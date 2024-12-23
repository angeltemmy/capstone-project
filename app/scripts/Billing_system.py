from confluent_kafka import Producer, Consumer
import json
import time
import datetime
from faker import Faker
from confluent_kafka import Producer
import json
import random
import time


# Function to read the configuration from a file
def read_config(config_file):
    config = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and not line.startswith("#"):
                parameter, value = line.split("=", 1)
                config[parameter.strip()] = value.strip()
    return config


fake = Faker()


# Generate a fake invoice
def get_random_customer():
    fake.unique.clear()  # Reset unique generator

    return {
        "CustomerID": fake.unique.random_int(1, 10000000),
        "Name": fake.name(),
      
        "address": fake.address().replace("\n", " "),
        "Email": fake.email(),
        "city": fake.city(),
        "country": fake.country(),
        }

def generate_product():
    fake.unique.clear()
    return {
        "ProductID": fake.unique.random_int(1, 5000000),
        "ProductName": fake.word(),
        "Description": fake.text(max_nb_chars=100),
        "Price": round(random.uniform(5.0, 1000.0), 2)
    }
def generate_taxrate():
    fake.unique.clear()
    return {
        "TaxID": fake.unique.random_int(1, 10000000),
        "TaxName": random.choice(["VAT", "Service Tax", "Sales Tax"]),
        "Rate": round(random.uniform(1.0, 20.0), 2)
    }

def generate_invoice(customer_id):
    fake.unique.clear()
    return {
        "InvoiceID": fake.unique.random_int(1, 2000),
        "CustomerID": customer_id,
        "InvoiceDate": fake.date_this_year().isoformat(),
        "DueDate": fake.date_this_year().isoformat(),
        "TotalAmount": round(random.uniform(100, 5000), 2)
    }

def generate_payment(invoice_id):

    return {
        "PaymentID": fake.unique.random_int(1, 30000000),
        "Invoices_InvoiceID": invoice_id,
        "PaymentAmount": round(random.uniform(100, 5000), 2),
        "PaymentDate": fake.date_this_month().isoformat(),
        "PaymentReference": fake.uuid4(),
        "PaymentStatus_StatusID": random.randint(1, 3),
        "PaymentMethods_MethodID": random.randint(1, 5)
    }

def generate_invoice_detail(invoice_id, product_id, tax_id):

    quantity = random.randint(1, 10000)
    return {
        "InvoiceID": invoice_id,
        "ProductID": product_id,
        "Quantity": quantity,
        "TaxID": tax_id,
        "LineTotal": round(quantity * random.uniform(10, 100), 2)
    }
def generate_bank_details(payment_id):

    return {
        "BankDetailID": fake.random_int(1, 50000),
        "Payments_PaymentID": payment_id,
        "BankName": fake.company(),
        "AccountNumber": fake.random_int(10000000, 99999999),
        "IBAN": fake.random_int(1000000000, 9999999999),
        "BIC": fake.random_int(100000, 999999)
    }


# Generate PaymentLogs
def generate_payment_logs(payment_id):
    fake.unique.clear()
    return {
        "LogID": fake.random_int(1, 50000),
        "Timestamp": fake.date_time_this_year().isoformat(),
        "LogMessage": fake.sentence(nb_words=8),
        "Payments_PaymentID": payment_id
    }


# Generate PaymentStatus
def generate_payment_status():
    fake.unique.clear()
    return {
        "StatusID": fake.random_int(1, 5),
        "StatusName": random.choice(["Pending", "Completed", "Failed", "Processing"]),
        "Description": fake.text(max_nb_chars=100)
    }


# Generate Discounts
def generate_discount():

    return {
        "DiscountID": fake.random_int(1, 10),
        "DiscountName": fake.word(),
        "DiscountValue": round(random.uniform(5.0, 50.0), 2)
    }


# Generate ShippingDetails
def generate_shipping_details(invoice_id):
    fake.unique.clear()
    return {
        "ShippingID": fake.unique.random_int(1, 5000000),
        "InvoiceID": invoice_id,
        "Address": fake.address().replace("\n", " "),
        "ShippingDate": fake.date_this_year().isoformat(),
        "EstimatedArrival": fake.future_date(end_date="+30d").isoformat()
    }


# Generate PaymentMethods
def generate_payment_method():
    fake.unique.clear()
    return {
        "MethodID": fake.unique.random_int(1, 100000),
        "MethodName": random.choice(["Credit Card", "PayPal", "Bank Transfer", "Cash", "Cryptocurrency"]),
        "Description": fake.text(max_nb_chars=50)
    }
# JSON Serializer
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


# Produce messages
def produce(topic, config):
    producer = Producer(config)
    print("Starting Producer...")
    try:
        while True:
            # Generate and send data for Customers
            customer = get_random_customer()
            producer.produce(topic, key="customer", value=json.dumps({"entity_type": "customer", **customer}))
            print(f"Sent Customer: {customer}")
            
           
            # Generate and send data for Products
            product = generate_product()
            producer.produce(topic, key="product", value=json.dumps({"entity_type": "product", **product}))
            print(f"Sent Product: {product}")

            # Generate and send Taxrates
            taxrate = generate_taxrate()
            producer.produce(topic, key="taxrate", value=json.dumps({"entity_type": "taxrate", **taxrate}))
            print(f"Sent Taxrate: {taxrate}")

            # Generate Invoices and related details
            invoice = generate_invoice(customer["CustomerID"])
            producer.produce(topic, key="invoice", value=json.dumps({"entity_type": "invoice", **invoice}))
            print(f"Sent Invoice: {invoice}")

            # Generate Payments for the invoice
            payment = generate_payment(invoice["InvoiceID"])
            producer.produce(topic, key="payment", value=json.dumps({"entity_type": "payment", **payment}))
            print(f"Sent Payment: {payment}")

            # Generate InvoiceDetails for products and tax
            invoice_detail = generate_invoice_detail(invoice["InvoiceID"], product["ProductID"], taxrate["TaxID"])
            producer.produce(topic, key="invoice_detail", value=json.dumps({"entity_type": "invoice_detail", **invoice_detail}))
            print(f"Sent InvoiceDetail: {invoice_detail}")

            payment_status = generate_payment_status()
            producer.produce(topic, key="payment_status", value=json.dumps({"entity_type": "payment_status", **payment_status}))
            print(f"Sent Payment Status: {payment_status}")

            # Generate and send Discounts
            discount = generate_discount()
            producer.produce(topic, key="discount", value=json.dumps({"entity_type": "discount", **discount}))
            print(f"Sent Discount: {discount}")

            # Generate and send Payment Methods
            payment_method = generate_payment_method()
            producer.produce(topic, key="payment_method", value=json.dumps({"entity_type": "payment_method", **payment_method}))
            print(f"Sent Payment Method: {payment_method}")

            # Generate Payment Logs and Bank Details linked to a PaymentID
            payment_id = fake.unique.random_int(1, 3000)
            payment_log = generate_payment_logs(payment_id)
            bank_detail = generate_bank_details(payment_id)
            producer.produce(topic, key="payment_log", value=json.dumps({"entity_type": "payment_log", **payment_log}))
            print(f"Sent Payment Log: {payment_log}")
            producer.produce(topic, key="bank_detail", value=json.dumps({"entity_type": "bank_detail", **bank_detail}))
            print(f"Sent Bank Detail: {bank_detail}")

            # Generate Shipping Details linked to an InvoiceID
            invoice_id = fake.unique.random_int(1, 2000)
            shipping_detail = generate_shipping_details(invoice_id)
            producer.produce(topic, key="shipping_detail", value=json.dumps({"entity_type": "shipping_detail", **shipping_detail}))
            print(f"Sent Shipping Detail: {shipping_detail}")
            
             # Flush producer buffer
            producer.flush()

            # Pause for a few seconds to simulate streaming
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()


# Consume messages
def consume(topic, config):
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"
    consumer = Consumer(config)
    consumer.subscribe([topic])
    print("Starting Consumer...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                key = msg.key().decode("utf-8") if msg.key() else "N/A"
                value = msg.value().decode("utf-8")
                print(f"Consumed: key = {key}, value = {value}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()


# Main function
def main():
    config = read_config("client.properties")
    topic = "Billing_system"

    # Choose to produce or consume
    print("Choose operation: 1 for Produce, 2 for Consume")
    choice = input("Enter choice: ").strip()
    if choice == "1":
        produce(topic, config)
    elif choice == "2":
        consume(topic, config)
    else:
        print("Invalid choice. Exiting.")


if __name__ == "__main__":
    main()
