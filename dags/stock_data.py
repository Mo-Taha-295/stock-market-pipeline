from airflow.hooks.base import BaseHook
from airflow.sdk import dag, task
from datetime import datetime , timedelta
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.slack.notifications.slack import SlackNotifier
from include.stock.extract import fetch_stock_data
from include.stock.store import store_stock_data
from include.stock.format import format_stock_data
from include.stock.validatecsv import validate_csv_exists 
from include.stock.load import load_to_dw


# Define reusable notifications for success and failure
success_notification = SlackNotifier(
    slack_conn_id="slack",
    text="Dag *{{dag.dag_id}}* completed successfully! for execution date {{ ds }} :tada:",
    channel="new-channel"
)
failure_notification = SlackNotifier(
    slack_conn_id="slack",
    text="Dag *{{ dag.dag_id }}* FAILED for execution date *{{ ds }}*!\n"
         "Check logs here: {{ ti.log_url }} :x:",
    channel="new-channel"
)

@dag(
    description="A DAG to fetch, store, format, validate and load stock data into a data warehouse.",
    schedule="@daily", 
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stock_data"] , 
    max_active_runs=1,
    max_consecutive_failed_dag_runs=3,
    on_success_callback=success_notification,
    on_failure_callback=failure_notification , 
    dagrun_timeout = timedelta(hours=2),
    default_args={
        "retries": 3,                          
        "retry_delay": timedelta(minutes=5),   
        "execution_timeout": timedelta(minutes=20),
        "email_on_failure":False, 
        "email_on_retry":False,
    }

)
def stock_market():

    @task.sensor(
            poke_interval=120, 
            timeout=360, 
            mode="reschedule",
            exponential_backoff=True,)
    def is_api_available():
        import requests
        from requests.exceptions import ConnectionError, HTTPError
        symbol = "nvda"  
        try:
            api = BaseHook.get_connection("stock_api")
            url = f"{api.host}{api.extra_dejson.get('endpoint')}{symbol}?metrics=high?&interval=1d&range=1y"
            r = requests.get(url, headers=api.extra_dejson.get('headers'))
            r.raise_for_status() 
            return PokeReturnValue(is_done=True, xcom_value=url) # if we reach this line, status MUST be 200
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
        return validate_csv_exists(formatted_path)

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