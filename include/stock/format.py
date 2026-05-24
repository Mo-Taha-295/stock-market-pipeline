from include.helpers.minio import get_minio_client
from airflow.hooks.base import BaseHook
from minio import Minio
import pandas as pd
import json
import io

#  path value e.g.-> stock-data/NVDA/prices_data.json

def format_stock_data(path : str) -> str:
    '''
    Format the stock data into a structured format (CSV).
    Args:
        data (str): The full path where the data is stored in MinIO.
    Returns:
        path: where the formatted data is stored in MinIO
    '''

    client = get_minio_client()
    
    symbol = path.split('/')[1]   #  e.g. → "NVDA"  
    bucket = path.split('/')[0]  # Extract bucket name from the path
    object_name = '/'.join(path.split('/')[1:])  # Extract object name from the path -> NVDA/prices_data.json
    

    response = client.get_object(bucket, object_name)
    file_data = response.read()
    response.close()    
    data = json.loads(file_data)  # Load the JSON data into a Python dictionary
    # Extract the relevant data and convert it to a DataFrame
    timestamps = data.get('timestamp', [])
    indicators = data.get('indicators', {}).get('quote', [])[0]
    df = pd.DataFrame({
        'timestamp': timestamps,
        'open': indicators.get('open', []),
        'high': indicators.get('high', []),
        'low': indicators.get('low', []),
        'close': indicators.get('close', []),
        'volume': indicators.get('volume', [])
    })
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)  # Convert the DataFrame to CSV format
    csv_string = csv_buffer.getvalue()
    csv_bytes = csv_string.encode('utf-8')  # string → bytes
    csv_object = f"{symbol}/prices_formatted.csv"
    objw = client.put_object(
        bucket_name=bucket,
        object_name=csv_object,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type='text/csv'
    )
    return f'{objw.bucket_name}/{objw.object_name}'  # Return the full path where the formatted data is stored in MinIO
    
