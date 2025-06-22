import random
from faker import Faker
from sqlalchemy import (
    Integer, String, Float, Boolean, Text, DateTime, Date, Time, Enum, Column
)
from sqlalchemy.sql.schema import Table
from sqlalchemy.dialects.postgresql import JSON, JSONB

fake = Faker()
import uuid
from sqlalchemy.dialects.postgresql import UUID  # Add this

def generate_fake_value(column: Column):
    """
    Generate a fake value based on column name and type using heuristics.
    """
    name = column.name.lower()
    col_type = column.type

    if "email" in name:
        return fake.email()
    if "password" in name:
        return fake.password(length=12)
    if "card" in name and "number" in name:
        return f"**** **** **** {fake.random_number(digits=3)}"
    if "phone" in name:
        return fake.phone_number()
    if "first_name" in name:
        return fake.first_name()
    if "last_name" in name:
        return fake.last_name()
    if "name" in name:
        return fake.name()
    if "address" in name:
        return fake.address()
    if "street" in name:
        return fake.street_address()
    if "city" in name:
        return fake.city()
    if "state" in name:
        return fake.state()
    if "country" in name:
        return fake.country()
    if "zip" in name or "postal" in name:
        return fake.postcode()

    if isinstance(col_type, UUID):  # ✅ Add this
        return str(uuid.uuid4())
    elif isinstance(col_type, Integer):
        return fake.random_int(min=1, max=1000)
    elif isinstance(col_type, Float):
        return round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2)
    elif isinstance(col_type, Boolean):
        return fake.boolean()
    elif isinstance(col_type, String):
        return fake.word()[:col_type.length] if col_type.length else fake.word()
    elif isinstance(col_type, Text):
        return fake.paragraph(nb_sentences=3)
    elif isinstance(col_type, DateTime):
        return fake.date_time_this_decade()
    elif isinstance(col_type, Date):
        return fake.date_this_decade()
    elif isinstance(col_type, Time):
        return fake.time_object()
    elif isinstance(col_type, Enum):
        return fake.random_element(elements=col_type.enums)
    return fake.word()


import random
import uuid
from faker import Faker
from sqlalchemy import Integer, String, Float, Boolean, Text, DateTime, Date, Time, Enum, Column
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql.schema import Table

fake = Faker()

def generate_fake_value(column: Column):
    name = column.name.lower()
    col_type = column.type

    if isinstance(col_type, UUID):
        return str(uuid.uuid4())
    if "email" in name:
        return fake.email()
    if "password" in name:
        return fake.password(length=12)
    if "card" in name and "number" in name:
        return f"**** **** **** {fake.random_number(digits=3)}"
    if "phone" in name:
        return fake.phone_number()
    if "first_name" in name:
        return fake.first_name()
    if "last_name" in name:
        return fake.last_name()
    if "name" in name:
        return fake.name()
    if "address" in name:
        return fake.address()
    if "street" in name:
        return fake.street_address()
    if "city" in name:
        return fake.city()
    if "state" in name:
        return fake.state()
    if "country" in name:
        return fake.country()
    if "zip" in name or "postal" in name:
        return fake.postcode()

    if isinstance(col_type, Integer):
        return fake.random_int(min=1, max=1000)
    elif isinstance(col_type, Float):
        return round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2)
    elif isinstance(col_type, Boolean):
        return fake.boolean()
    elif isinstance(col_type, String):
        return fake.word()[:col_type.length] if col_type.length else fake.word()
    elif isinstance(col_type, Text):
        return fake.paragraph(nb_sentences=3)
    elif isinstance(col_type, DateTime):
        return fake.date_time_this_decade()
    elif isinstance(col_type, Date):
        return fake.date_this_decade()
    elif isinstance(col_type, Time):
        return fake.time_object()
    elif isinstance(col_type, Enum):
        return fake.random_element(elements=col_type.enums)
    elif isinstance(col_type, (JSON, JSONB)):
        return {
            "example_key": fake.word(),
            "value": fake.random_int(min=1, max=100),
            "nested": {"inner_key": fake.word()}
        }


    return fake.word()



def generate_fake_row(table: Table, primary_keys_data: dict) -> dict:
    """
    Generate a single fake row, resolving foreign keys using existing inserted primary key data.
    """
    row = {}

    for column in table.columns:
        if column.primary_key and getattr(column, "autoincrement", False):
            continue
        elif column.foreign_keys:
            fk = list(column.foreign_keys)[0]
            ref_table = fk.column.table.name
            ref_column = fk.column.name
            
            if ref_table not in primary_keys_data or ref_column not in primary_keys_data[ref_table]:
                raise ValueError(f"No primary key values found for FK: {column.name} → {ref_table}.{ref_column}")

            possible_values = primary_keys_data[ref_table][ref_column]
            # print(f"The primary_keys are =>{primary_keys_data}")
            if not possible_values:
                raise ValueError(f"No values available for foreign key: {ref_table}.{ref_column}")

            row[column.name] = random.choice(possible_values)

        else:
            row[column.name] = generate_fake_value(column)

    return row
