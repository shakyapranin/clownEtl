# Connect to s3
# Read files from landing folder
# Classify files
# Determine date from parsing filename
# Create a new table as client_name_filetype_date
import os
from airflow import DAG
from dotenv import load_dotenv
from datetime import timedelta, datetime
from airflow.decorators import task
from airflow.utils.db import provide_session
from airflow.models import Connection
from airflow.utils.dates import days_ago

load_dotenv()
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = os.getenv('REGION_NAME')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@provide_session
def create_s3_connection(session=None) -> None:
    connection_id = f'aws_s3_connection_{datetime.now().isoformat()}'
    connection_type = 'aws'
    extra = {
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "region_name": region_name,
    }

    # Delete connection if it already exists
    # TODO: Figure out a better way to do this
    existing_connection = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_connection:
        session.delete(existing_connection)
        session.commit()
    
    new_connection = Connection(
        conn_id = connection_id,
        conn_type = connection_type,
        extra = extra
    )

    session.add(new_connection)
    session.commit()
    print(f"Connection {connection_id} created")


with DAG(
    's3_to_postgres_with_s3_conn',
    default_args=default_args,
    description='Load CSV data from S3 to Postgres with dynamic S3 connection',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    @task
    def setup_s3_connection() -> None:
        create_s3_connection()


    setup_s3_conn = setup_s3_connection()


