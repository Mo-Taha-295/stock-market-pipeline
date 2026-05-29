from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from io import BytesIO
from include.helpers.minio import get_minio_client

def store_stock_data(data):
    '''
    Store stock data in a MinIO file system.
    Args:
        data (JSON): The stock data to be stored.
    Returns:
        str: The path to the stored stock data in MinIO.
    '''

    s3_hook = S3Hook(aws_conn_id='minio_conn') # Create an S3 hook to interact with MinIO which is compatible with S3 API
    bucket_name = 'stock-data' # Define the bucket name where the data will be stored
    if not s3_hook.check_for_bucket(bucket_name = bucket_name):
        s3_hook.create_bucket (bucket_name = bucket_name)
        print(f"Bucket '{bucket_name}' successfully created!")
    else : 
        print(f'Bucket {bucket_name} already exists.')

    stock = json.loads(data)
    symbol = stock.get('meta', {}).get('symbol', 'unknown_stock') # Extract the stock symbol from the data
    data = json.dumps(stock , ensure_ascii=False).encode('utf-8')  # encode the stock data back to a JSON 
    target_path =  f'NVDA/{symbol}_prices_data.json'
    # Pass raw bytes directly without any wrappers
    s3_hook.load_bytes(
    bytes_data=data,
    key=target_path,
    bucket_name=bucket_name,
    replace=True
                      )
     
    return f"{bucket_name}/{target_path}"
    

