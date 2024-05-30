import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, insert
import json

# Define the path to the CSV file
csv_file_path = 'docs/client_data.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Display the DataFrame (optional)
print(df)

# Create a database engine
engine = create_engine('sqlite:///etl_pipeline.db')
metadata = MetaData()

# Define table schemas
raw_table = Table('raw_data', metadata,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('client_id', Integer),
                  Column('data', String))

staging_table = Table('staging_data', metadata,
                      Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('client_id', Integer),
                      Column('first_name', String),
                      Column('last_name', String),
                      Column('email', String),
                      Column('phone', String),
                      Column('created_at', String))

dim_client_table = Table('dim_client', metadata,
                         Column('client_id', Integer, primary_key=True),
                         Column('first_name', String),
                         Column('last_name', String),
                         Column('email', String),
                         Column('phone', String))

fact_data_table = Table('fact_data', metadata,
                        Column('id', Integer, primary_key=True, autoincrement=True),
                        Column('created_at', String),
                        Column('client_id', Integer, nullable=False))

# Create tables
metadata.create_all(engine)

# Load DataFrame into the raw_data table
# TODO: Is the sql engine connecting twice?
with engine.connect() as conn:
    for _, row in df.iterrows():
        stmt = insert(raw_table).values(client_id=row['client_id'], data=row['data'])
        conn.execute(stmt)

# Function to process data from raw to staging
def process_raw_to_staging():
    with engine.connect() as conn:
        raw_data = conn.execute(raw_table.select()).fetchall()
        staging_data = []

        for row in raw_data:
            client_id = row['client_id']
            data = json.loads(row['data'].replace('""', '"'))
            staging_data.append({
                'client_id': client_id,
                'first_name': data.get('first_name'),
                'last_name': data.get('last_name'),
                'email': data.get('email'),
                'phone': data.get('phone'),
                'created_at': data.get('created_at')
            })
        
        for data in staging_data:
            stmt = insert(staging_table).values(**data)
            conn.execute(stmt)

# Process raw data to staging
process_raw_to_staging()

# Function to process data from staging to dimension and fact tables
def process_staging_to_dim_fact():
    with engine.connect() as conn:
        staging_data = conn.execute(staging_table.select()).fetchall()
        
        # Processing dimensions
        dim_client_data = []
        fact_data = []
        for row in staging_data:
            dim_client_data.append({
                'client_id': row['client_id'],
                'first_name': row['first_name'],
                'last_name': row['last_name'],
                'email': row['email'],
                'phone': row['phone']
            })
            fact_data.append({
                'client_id': row['client_id'],
                'created_at': row['created_at']
            })
        
        for data in dim_client_data:
            stmt = insert(dim_client_table).values(**data)
            try:
                conn.execute(stmt)
            except:
                # Ignore duplicate client_ids
                pass
        
        for data in fact_data:
            stmt = insert(fact_data_table).values(**data)
            conn.execute(stmt)

# Process staging data to dimension and fact tables
process_staging_to_dim_fact()

# Display message
print("Data processed and loaded into staging, dimension, and fact tables")
