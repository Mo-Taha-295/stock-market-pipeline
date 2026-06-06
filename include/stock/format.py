from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import json



def format_stock_data(path):
    '''
    Format the stock data into a structured format (CSV).
    Args:
        data (str): The full path where the data is stored in MinIO.
    Returns:
        path: where the formatted data is stored in MinIO
    '''

    # initiate the S3 hook to interact with MinIO
    s3_hook = S3Hook(aws_conn_id='minio_conn')

    # path value e.g.-> stock-data/NVDA/prices_data.json
    symbol = path.split('/')[1]   #  e.g. → "NVDA"  
    bucket = path.split('/')[0]  # Extract bucket name from the path
    json_key = '/'.join(path.split('/')[1:])  # Extract object name from the path -> NVDA/prices_data.json
    
    file_data = s3_hook.read_key(key=json_key, bucket_name=bucket)  # Read the JSON data from MinIO using the S3 hook

   
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
    
    # Convert the DataFrame to a CSV string
    csv_string = df.to_csv(index=False)

    # Define the target path for the formatted CSV file in MinIO
    csv_key = f"{symbol}/{symbol}_prices_formatted.csv"
    s3_hook.load_bytes(
        bucket_name=bucket,
        key=csv_key,
        bytes_data=csv_string.encode('utf-8'),
        replace=True
    )

    
    return f'{bucket}/{csv_key}'  # Return the full path where the formatted data is stored in MinIO
    

