from airflow.exceptions import AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def validate_csv_exists(path):
    '''
    Validate that a CSV file exists at the specified path in MinIO.
    Args:
        path (str): The full path where the formatted data is stored in MinIO.
    Returns:
        str: The same path if the file exists.
    '''

    # path value e.g.-> stock-data/NVDA/prices_formatted.csv

    s3_hook = S3Hook(aws_conn_id='minio_conn')
    # bucket_name = path.split('/')[0]  # Extract bucket name from the path
    # key = '/'.join(path.split('/')[1:])  # Extract object name from the path
    bucket_name, key = path.split("/", 1)

    if not s3_hook.check_for_key(key=key, bucket_name=bucket_name):
        raise AirflowNotFoundException(
            f"CSV file not found at {path}"
        )

    return path 