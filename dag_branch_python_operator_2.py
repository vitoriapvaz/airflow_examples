from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def avalia_task():
    var = Variable.get("carga")
    return f"carga_{var}"

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
    dag_id="dag_branch_python_operator_2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["variables"]
) as dag:

    branch_task = BranchPythonOperator(task_id="branch_task",python_callable=avalia_task)

    carga_historica = BashOperator(task_id="carga_historica",bash_command="echo 'Carga Historica'",dag=dag)
    carga_incremental = BashOperator(task_id="carga_incremental",bash_command="echo 'Carga Incremental'",dag=dag)

branch_task >> [carga_historica, carga_incremental]