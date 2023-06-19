from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_result(**context):
    result = context["ti"].xcom_pull(task_ids="query_data")
    for item in result:
        print(item)


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
    dag_id="dag_sql_operator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    create_table = SqliteOperator(task_id="create_table",
                                  sqlite_conn_id="sqlite_default",
                                  sql="CREATE TABLE IF NOT EXISTS table_teste (id INT);")

    insert_data = SqliteOperator(task_id="insert_data",
                                 sqlite_conn_id="sqlite_default",
                                 sql="INSERT INTO table_teste VALUES (1);")

    query_data = SqliteOperator(task_id="query_data",
                                sqlite_conn_id="sqlite_default",
                                sql="SELECT * FROM table_teste;")

    print_result_task = PythonOperator(task_id="print_result",
                                       python_callable=print_result)


create_table >> insert_data >> query_data >> print_result_task
