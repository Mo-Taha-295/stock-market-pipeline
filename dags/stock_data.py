from airflow.hooks.base import BaseHook
from airflow.sdk import dag, task
from datetime import datetime
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.slack.notifications.slack import SlackNotifier
from include.stock.extract import fetch_stock_data
from include.stock.store import store_stock_data
from include.stock.format import format_stock_data
from include.stock.getformatted import get_formatted_csv
from include.stock.load import load_to_dw


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2} ,
    tags=["stock_data"] , 
    max_active_runs=1,
#     on_success_callback=SlackNotifier(
#         slack_conn_id="slack",
#         text="Stock data pipeline completed successfully! :tada:",
#         channel="new-channel"
# ),  
# on_failure_callback=SlackNotifier(
#         slack_conn_id="slack",
#         text="Stock data pipeline failed! :x:",
#         channel="new-channel"
# )

)
def stock_market():

    @task.sensor(poke_interval=60, timeout=360)
    def is_api_available():
        import requests
        from requests.exceptions import ConnectionError, HTTPError
        symbol = "nvda"  
        try:
            api = BaseHook.get_connection("stock_api")
            url = f"{api.host}{api.extra_dejson.get('endpoint')}{symbol}?metrics=high?&interval=1d&range=1y"
            r = requests.get(url, headers=api.extra_dejson.get('headers'))
            r.raise_for_status()
            return PokeReturnValue(is_done=r.status_code == 200, xcom_value=url)
        except ConnectionError:
            return PokeReturnValue(is_done=False)
        except HTTPError as err:
            print(f"Server error: {err}")
            return PokeReturnValue(is_done=False)
        except Exception as e:
            print(f"Unexpected error: {e}")
            return PokeReturnValue(is_done=False)

    @task
    def fetch_stock_prices(url: str):
        return fetch_stock_data(url)

    @task
    def store_stock_prices(data: dict):
        return store_stock_data(data)

    @task
    def format_stock_prices(path: str):
        return format_stock_data(path)

    @task
    def formatted_csv(formatted_path: str):
        return get_formatted_csv(formatted_path)

    @task
    def load_to(formatted_csv_path: str):
        return load_to_dw(formatted_csv_path)


    url            = is_api_available()
    data           = fetch_stock_prices(url)
    path           = store_stock_prices(data)
    formatted_path = format_stock_prices(path)
    formatted_path_= formatted_csv(formatted_path) 
    dw_load        = load_to(formatted_path_)


stock_market()