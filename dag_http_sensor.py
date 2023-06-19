from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
import requests

def query_api():
    response = requests.get("https://api.publicapis.org/entries")
    print(response.text)

default_args = {
    "owner": "vitoria",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 16),
    "email": ["maria.vaz@123milhas.com.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="dag_http_sensor",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    check_api = HttpSensor(task_id="check_api",
                http_conn_id="connection_api_example",
                endpoint="entries",
                poke_interval=5,
                timeout=20)
    process_data = PythonOperator(task_id="process_data", python_callable=query_api)

check_api >> process_data