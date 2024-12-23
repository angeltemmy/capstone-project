
import subprocess

def main():
    print("Choose producer to start:")
    print("1. Customer Producer")
    print("2. Product Producer")
    print("3. TaxRate Producer")
    print("4. Invoice Producer")
    print("5. Payment Producer")
    print("6. Invoice Detail Producer")
    print("7. Bank Details Producer")
    print("8. Payment Logs Producer")
    print("9. Payment Status Producer")
    print("10. Discount Producer")
    print("11. Shipping Details Producer")
    print("12. Payment Method Producer")

    choice = input("Enter choice: ").strip()

    if choice == "1":
        subprocess.run(["python", "customer_producer.py"])
    elif choice == "2":
        subprocess.run(["python", "product_producer.py"])
    elif choice == "3":
        subprocess.run(["python", "taxrate_producer.py"])
    elif choice == "4":
        subprocess.run(["python", "invoice_producer.py"])
    elif choice == "5":
        subprocess.run(["python", "payment_producer.py"])
    elif choice == "6":
        subprocess.run(["python", "invoice_detail_producer.py"])
    elif choice == "7":
        subprocess.run(["python", "bank_details_producer.py"])
    elif choice == "8":
        subprocess.run(["python", "payment_logs_producer.py"])
    elif choice == "9":
        subprocess.run(["python", "payment_status_producer.py"])
    elif choice == "10":
        subprocess.run(["python", "discount_producer.py"])
    elif choice == "11":
        subprocess.run(["python", "shipping_details_producer.py"])
    elif choice == "12":
        subprocess.run(["python", "payment_method_producer.py"])
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()
