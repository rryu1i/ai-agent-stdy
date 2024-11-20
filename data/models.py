from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

class CustomersSchema(BaseModel):
    customer_id: str
    customer_unique_id: str
    customer_zip_code_prefix: str
    customer_city: str
    customer_state: str

    class Config:
        orm_mode = True


Base = declarative_base()

class Customers(Base):
    __tablename__ = "customers"

    customer_id = Column(String, primary_key=True)
    customer_unique_id = Column(String)
    customer_zip_code_prefix = Column(String)
    customer_city = Column(String)
    customer_state = Column(String)