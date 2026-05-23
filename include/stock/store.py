from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO
from include.helpers.minio import get_minio_client

def store_stock_data(data):
    '''
    Store stock data in a MinIO file system.
    Args:
        data (JSON): The stock data to be stored.
    '''

    client = get_minio_client()
    
    bucket_name = "stock-data"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    stock = json.loads(data) if hasattr(data, "read") else json.loads(data) # decode the JSON data into a Python dict
    symbol = stock.get('meta', {}).get('symbol', 'unknown_stock') # Extract the stock symbol from the data
    data = json.dumps(stock , ensure_ascii=False).encode('utf-8')  # encode the stock data back to a JSON string for storage
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices_data.json",
        data=BytesIO(data),
        length=len(data)
    ) 
    return f'{objw.bucket_name}/{objw.object_name}' # return the full path where the data is stored in MinIO