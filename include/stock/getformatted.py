from include.helpers.minio import get_minio_client
from airflow.exceptions import AirflowNotFoundException

def get_formatted_csv(path):
    '''
    Get the formatted stock data as a CSV.
    Args:
        path (str): The full path where the formatted data is stored in MinIO.
    Returns:
        csv: The formatted stock data as a CSV.
    '''

    # path value e.g.-> stock-data/NVDA/prices_formatted.csv

    client = get_minio_client()
    bucket_name = path.split('/')[0]  # Extract bucket name from the path
    object_name = '/'.join(path.split('/')[1:])  # Extract object name from the path

    try:
        client.stat_object(bucket_name, object_name)
        return path  # ← just the path, no data!    except Exception as e:
    except Exception as e:

        raise AirflowNotFoundException(
            f"CSV file not found at {path}: {e}"
        )


