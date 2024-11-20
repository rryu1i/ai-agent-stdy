from pydantic import BaseModel, ValidationError
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class Engine():
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = None


    def create_engine(self):
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.engine = create_engine(connection_string, echo=True)
        return self.engine
    

    def create_session(self):
        if self.engine is None:
            self.create_engine()
        Session = sessionmaker(bind=self.engine)
        return Session()
    

    def create_all_tables(self, base):
        if self.engine is None:
            self.create_engine()
        return base.metadata.create_all(self.engine)


class DataPreProcessing():
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
    

    def read_csv_data(self, csv_file: str, dtype: dict = None):
        csv_file_path = os.path.join(self.csv_path, csv_file)
        return pd.read_csv(csv_file_path, dtype=dtype)
    

    def validate_data(self, df: pd.DataFrame, schema: BaseModel):
        data_records = []
        errors = []
        for index, row in df.iterrows():
            try:
                record = schema(**row.to_dict())
                data_records.append(record.dict())
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                errors.append(index)
        return pd.DataFrame(data_records), errors
        