from airflow.hooks.base import BaseHook
import requests , json


def fetch_stock_data(url: str):
    '''Fetch stock data from the API and return it as a JSON file.'''
    
    api = BaseHook.get_connection("stock_api")
    r = requests.get(url , headers=api.extra_dejson.get('headers'))
    print(f"Fetched data successfully with status code: {r.status_code}")
    return json.dumps(r.json()['chart']['result'][0])  # Return the JSON response as json file

