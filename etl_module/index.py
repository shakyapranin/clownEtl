import pandas as pd
import Logger
import Metadata
import dispatcher

BASE_URL = "http://s3.amazonaws.com/AmazonS3/crown-health/etl-files"

engine = create_engine('sqlite:///etl_pipeline.db')

def start_etl():
    """
    For all clients
    1. Iterate through the list of clients
    2. Get url for client specific folder (identify the type of file)
    3. Read flat files
    4. (Skipped) Standardize the file naming
    5. Identify and classify as Account, financial, BillReference, Insurance
    6. Create a new table in raw schema as client_name_fileType_date and load data
    7. Log actions into log table
    8. (Skipped) Check for staging data - Clear staging data if it already exists
    9. Process the raw table with a view (Pre existing)
    10. Process the raw table into dimensions and facts tables
    11. Log the number of rows from file imported
    12. Log the number of rows that got into staging table
    13. ASYNC Move the files into archive folder
    """
    # 1
    clients: list = [{id: 1, name: "Hospital 1"}, {id: 2, name: "Hospital 2"}] # Todo fetch the list of clients from database
    # 2
    for client in clients:
        # TODO: WRAP all of these into a try catch block
        client_bucket_name = BASE_URL + client.name
        # 3
        files = read_all_objects(client_bucket_name)
        for file in files:
            # 5
            file_type = identify_file(file)

            # 6
            time = int(time.time() * 1000) 
            client_name = client.name
            table_name = f"{client_name}_{file_type}_{int(time.time() * 1000)}"
            create_raw_table(table_name)
            data = pd.read_csv(file_path)
            num_rows = len(data)
            data.to_sql(table_name, engine, if_exists='append', index=False)

            # 7 and 11
            create_log(f"Created raw table: {table_name} and imported with {num_rows} rows.")

            # 8 Skipped

            # 9 Compose view as per pypika for transformation
            table = Table(table_name)
            view_query = Query.from_(table).select(client.id, client_name).where(client.active==True)
            view_query_str = str(view_query)
            view_table_name = f"{table_name}_view"
            create_view_query = f"CREATE VIEW {view_table_name} AS {view_query_str}"

            cursor = connection.cursor()
            cursor.execute(create_view_query)

            client_staging_table = f"staging_{client_name}"
            insert_from_view_query = f"INSERT INTO {client_staging_table} select * from {view_table_name}"

            rows_affected = insert_from_view_query.rowcount
            create_log(f"Imported {rows_affected} rows into staging table: {client_staging_table}.")

            connection.commit()

            #13
            dispatcher("archive_file", meta: { bucket_name: {client_bucket_name}, destination_path: f"/archive/{time}/" , source_path: file.path })


def dispatcher(event_type, meta):
    queue = new SQS() # We can may be create a queue of APSceduler
    queue.add(queue: event_type, meta)

# Should be a lambda function triggered by SQS may be?
def queue_execute(event_type, meta):
    caller.getAttribute(event_type)(*meta) # assume we will call archive_file

def archive_file(bucket_name, source_path, destination_path):
    move_object(bucket_name, source_path, destination_path)


def create_log(message: str, log_type: str):
    logger = new Logger()
    return logger.getAttribute(log_type)(message)
           

def create_raw_table(table_name) -> str:
    """
    Create a raw table for a client file type at certain point of time
    """
    metadata = new Metadata()
    raw_table = Table(table_name, metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('client_id', Integer),
        Column('data', String))
    try: 
        metadata.create_all(engine)
        return table_name
    except: 
        raise

def identify_file(file):
    """
    Iterate over client file and determine type by file name
    """
    match True:
        case file.file_name.includes("account"):
            return "Account"
        case file.file_name.includes("financial"):
            return "Financial"
        case file.file_name.includes("bill"):
            return "BillReference"
        case file.file_name.includes("insurance"):
            return "Insurance"
        case _:
            return "Unidentified"