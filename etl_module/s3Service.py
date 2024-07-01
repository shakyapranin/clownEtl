import boto3
import botocore

s3 = boto3.resource('s3')

def read_object(bucket_name: str, object_key: str):
    try:
        object_ = s3.Object(bucket_name, object_key)

        return object_.get()['Body'].read().decode('utf-8')
    except botocore.exceptions.ClientError as e:
        # If the error is a 404 error
        if e.response['Error']['Code'] == "404":
            print(f"The object {object_key} does not exist in bucket {bucket_name}.")
        else:
            raise

def read_all_objects(bucket_name: str) -> Generator[Any, Any, None]: 
    try:
        bucket = s3.Bucket(bucket_name)
        for object_ in bucket.objects.all():
            yield object_
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"The bucket {bucket_name} does not exist.")
        else:
            raise

# Function to move objects into a folder
def move_objects(bucket_name, source_prefix, destination_prefix):
    try:
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=source_prefix):
            # Define source and destination keys
            source_key = obj.key
            destination_key = destination_prefix + source_key[len(source_prefix):]

            # Copy the object
            copy_source = {'Bucket': bucket_name, 'Key': source_key}
            s3.Object(bucket_name, destination_key).copy(copy_source)

            # Delete the original object
            s3.Object(bucket_name, source_key).delete()

            print(f'Moved {source_key} to {destination_key}')
    except botocore.exceptions.ClientError as e:
        print(f'Error: {e}')


# Function to move a single object to a new folder
def move_object(bucket_name, source_key, destination_key):
    try:
        # Copy the object
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3.Object(bucket_name, destination_key).copy(copy_source)

        # Delete the original object
        s3.Object(bucket_name, source_key).delete()

        print(f'Moved {source_key} to {destination_key}')
    except botocore.exceptions.ClientError as e:
        print(f'Error: {e}')


