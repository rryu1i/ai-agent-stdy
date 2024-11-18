import os
from data.models import Reviews
from data.data_ingestion import DataPreProcessing, DataIngestion

CSV_PATH = "data/raw_data"
CSV_FILE = "olist_order_reviews_dataset.csv" 
TABLE_NAME = "reviews"

# PostgreSQL Database Configuration (fetch from environment variables)
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# Table schema definition
REVIEWS_TABLE_SCHEMA = """
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    review_score INTEGER NOT NULL,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP
"""

def main():
    preprocessor = DataPreProcessing(CSV_PATH)
    print("Reading CSV data...")
    raw_data = preprocessor.read_csv_data(CSV_FILE)
    print(f"Read {len(raw_data)} rows from {CSV_FILE}.")

    print("Validating data...")
    validated_data, validation_errors = preprocessor.validate_data(raw_data, Reviews)

    if validation_errors:
        print(f"Validation completed with {len(validation_errors)} errors.")
        for error in validation_errors[:10]:
            print(error)
    else:
        print("All data validated successfully.")

    ingestion = DataIngestion(**DB_CONFIG)
    print(f"Creating table '{TABLE_NAME}' in the database...")
    ingestion.create_table(TABLE_NAME, REVIEWS_TABLE_SCHEMA)

    if not validated_data.empty:
        print(f"Inserting {len(validated_data)} rows into the database...")
        ingestion.write_table(validated_data, TABLE_NAME)
        print("Data ingestion completed successfully.")
    else:
        print("No valid data to insert.")

if __name__ == "__main__":
    main()