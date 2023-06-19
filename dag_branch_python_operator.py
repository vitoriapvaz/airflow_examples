from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import random

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

def gera_numero_aleatorio(**context):
    number = random.randint(1, 100)
    context["ti"].xcom_push(key="numero_gerado",value=number)


def avalia_numero_aleatorio(**context):
    number = int(context["ti"].xcom_pull(task_ids="gera_numero_aleatorio_task", key="numero_gerado"))
    if number % 2 == 0:
        return "par_task"
    else:
        return "impar_task"

with DAG(
    dag_id="dag_branch_python_operator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["xcom"]
) as dag:

    gera_numero_aleatorio_task = PythonOperator(task_id="gera_numero_aleatorio_task", python_callable = gera_numero_aleatorio,provide_context=True)
    branch_task = BranchPythonOperator(task_id="branch_task",python_callable=avalia_numero_aleatorio,provide_context=True)

    par_task = BashOperator(task_id="par_task",bash_command="echo 'Número Par' & sleep 10")
    impar_task = BashOperator(task_id="impar_task",bash_command="echo 'Número Impar' & sleep 10")

gera_numero_aleatorio_task >> branch_task >> [par_task, impar_task]