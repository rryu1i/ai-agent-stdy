import os
from data.models import Customers
from data.models import Base
from data.models import CustomersSchema
from data.data_ingestion import Engine, DataPreProcessing


def main():

    CSV_PATH = "data/raw_data"
    CSV_FILE = "olist_customers_dataset.csv" 
    DB_CONFIG = {
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    columns_to_cast = {"customer_zip_code_prefix": str}
    df_1 = DataPreProcessing(CSV_PATH).read_csv_data(CSV_FILE, dtype=columns_to_cast)
    df, errors = DataPreProcessing(CSV_PATH).validate_data(df_1, CustomersSchema)

    session = Engine(**DB_CONFIG).create_session()
    Engine(**DB_CONFIG).create_all_tables(Base)

    for _, record in df.iterrows():
        session.add(Customers(**record))

    session.commit()
    session.close()

if __name__ == "__main__":
    main()