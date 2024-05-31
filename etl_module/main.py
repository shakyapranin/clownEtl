import json
import time
import pandas as pd
from sqlalchemy import Column, Integer, String, Table, create_engine, insert, MetaData, text
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Database connection
engine = create_engine('sqlite:///etl_pipeline.db')

# Directory to monitor
client_directory = "docs"
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
                         Column('full_name', String),
                         Column('email', String),
                         Column('phone', String))

fact_data_table = Table('fact_data', metadata,
                        Column('id', Integer, primary_key=True, autoincrement=True),
                        Column('created_at', String),
                        Column('client_id', Integer, nullable=False))

# Create tables
metadata.create_all(engine)

# Load raw data
def load_raw_data(file_path):
    data = pd.read_csv(file_path)
    data.to_sql('raw_data', engine, if_exists='append', index=False)
    print(f"Loaded raw data from {file_path}")
    return data

# Process raw data to staging
def process_to_staging(conn, raw_data):
    staging_data = []
    for _, row in raw_data.iterrows():
        row = dict(row)
        client_id = row['client_id']
        data = json.loads(row['data'])
        staging_data.append({
            'client_id': client_id,
            'first_name': data.get('first_name'),
            'last_name': data.get('last_name'),
            'email': data.get('email'),
            'phone': data.get('phone'),
            'created_at': data.get('created_at')
        })
    stmt = staging_table.insert().values(staging_data)
    result = conn.execute(stmt)
    conn.commit()
    print("Processed data to staging")
    return staging_data

# Process staging data to dimension and fact tables
def process_to_dimension_fact():
    with engine.connect() as conn:
        staging_data = pd.read_sql("SELECT * FROM staging_data", conn)
        clients = staging_data[['client_id']].drop_duplicates()
        clients['full_name'] = staging_data['first_name'] + ' ' + staging_data['last_name']
        clients['email'] = staging_data['email']
        clients['phone'] = staging_data['phone']
        clients['client_name'] = clients['client_id'].apply(lambda x: f"Client {x}")  # Example transformation
        clients.to_sql('dim_client', engine, if_exists='replace', index=False)
        staging_data[['id', 'client_id', 'created_at']].to_sql('fact_data', engine, if_exists='replace', index=False)
    print("Processed data to dimension and fact tables")

# Watchdog event handler
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".csv"):
            load_raw_data(event.src_path)
            with engine.connect() as conn:
                raw_data = pd.read_sql("SELECT * FROM raw_data", conn)
                process_to_staging(conn, raw_data)
            process_to_dimension_fact()

# Main ETL pipeline
def main():
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, client_directory, recursive=False)
    observer.start()
    print(f"Monitoring directory: {client_directory}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
