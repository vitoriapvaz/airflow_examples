from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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

dag = DAG(
    dag_id="dag_sequencial",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

task1 = BashOperator(task_id="tsk1", bash_command="echo 'Sou a task 1' & sleep 5", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command="echo 'Sou a task 2' & sleep 5", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command="echo 'Sou a task 3' & sleep 5", dag=dag)

task1 >> task2 >> task3

