# from minio import Minio
# from airflow.hooks.base import BaseHook

# def get_minio_client():
#     minio_conn = BaseHook.get_connection("minio_conn") 
#     client = Minio(
#         endpoint=minio_conn.extra_dejson.get('endpoint_url').split('//')[1],  # Extract host:port from the URL
#         access_key=minio_conn.login,
#         secret_key=minio_conn.password,
#         secure=False  # Set to True if using HTTPS
#     )
#     return client