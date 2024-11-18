import psycopg2
from pydantic import BaseModel, ValidationError
import pandas as pd
import os


class DataPreProcessing():
    def __init__(self, data_path):
        self.data_path = data_path

    def read_csv_data(self, filename):
        df = pd.read_csv(os.path.join(self.data_path, filename))
        return df
    
    @staticmethod
    def validate_data(df, model):
        validated_rows = []
        errors = []
        for _, row in df.iterrows():
            try:
                validated_instance = model(**row.to_dict())
                validated_rows.append(validated_instance.dict())
            except ValidationError as e:
                errors.append(f"Validation error in row {_}: {e}")
        return pd.DataFrame(validated_rows), errors


class DataIngestion():
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            return conn
        except psycopg2.Error as e:
            print(f"Database connection error: {e}")
            return None

    def create_table(self, table_name, schema):
        conn = self.connect()
        if conn is None:
            return
        cursor = conn.cursor()
        try:
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
            cursor.execute(query)
            conn.commit()
            print(f"Table '{table_name}' created or already exists.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
        finally:
            cursor.close()
            conn.close()

    def read_table(self, table_name):
        conn = self.connect()
        if conn is None:
            return None
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name};"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=colnames)
            return df
        except psycopg2.Error as e:
            print(f"Error reading table: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def write_table(self, df, table_name):
        conn = self.connect()
        if conn is None:
            return
        cursor = conn.cursor()

        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(df.columns)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            for row in df.itertuples(index=False):
                cursor.execute(query, row)
            conn.commit()
            print(f"Data inserted into {table_name}")
        except psycopg2.Error as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()