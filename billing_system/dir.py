import os

# List of table names
tables = [
    "customer",
    "discounts",
    "invoice_details",
    "invoices",
    "payment_logs",
    "payment_methods",
    "payment_status",
    "payments",
    "product",
    "shipping_details",
    "taxrates",
]

# Base directory for models
base_dir = "models"

# Create the directories and files for each table
for table in tables:
    table_dir = os.path.join(base_dir, table)
    os.makedirs(table_dir, exist_ok=True)  # Create directory
    # Create the SQL file
    with open(os.path.join(table_dir, f"{table}.sql"), "w") as sql_file:
        sql_file.write(f"-- SQL logic for the {table} table\n")
    # Create an empty schema.yml file
    with open(os.path.join(table_dir, "schema.yml"), "w") as schema_file:
        schema_file.write(f"")  # Leave the file empty

base_dir
