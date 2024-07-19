from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String

# Database connection
DATABASE_URI = 'sqlite:///etl_pipeline.db'
engine = create_engine(DATABASE_URI)
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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency':1
}

@dag(
    default_args=default_args,
    description='An ETL pipeline for loading and processing client data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def etl_pipeline(): 
    @task()
    def setup():
        metadata.create_all(engine)

    @task()
    def load_raw_data(file_path: str):
        data = pd.read_csv(file_path)
        data.to_sql('raw_data', engine, if_exists='append', index=False)
        print(f"Loaded raw data from {file_path}")
        return data

    @task()
    def process_to_staging():
        with engine.connect() as conn:
            raw_data = pd.read_sql("SELECT * FROM raw_data", conn)
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
            conn.execute(stmt)
            conn.commit()
            print("Processed data to staging")
        return staging_data

    @task()
    def process_to_dimension_fact():
        with engine.connect() as conn:
            staging_data = pd.read_sql("SELECT * FROM staging_data", conn)
            clients = staging_data[['client_id', 'first_name', 'last_name', 'email', 'phone']].drop_duplicates()
            clients['full_name'] = clients['first_name'] + ' ' + clients['last_name']
            clients.to_sql('dim_client', engine, if_exists='replace', index=False)
            staging_data[['id', 'client_id', 'created_at']].to_sql('fact_data', engine, if_exists='replace', index=False)
        print("Processed data to dimension and fact tables")

    # Define the file path (update as needed)
    file_path = './docs/client_data.csv'
    
    # Task execution order
    setup() >> load_raw_data(file_path) >> process_to_staging() >> process_to_dimension_fact()
    # raw_data = load_raw_data(file_path)
    # staging_data = process_to_staging()
    # process_to_dimension_fact()

etl_dag = etl_pipeline()
