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

with DAG(
    dag_id="dag_trigger_one_failed",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = BashOperator(task_id="tsk1", bash_command="sleep 5")
    task2 = BashOperator(task_id="tsk2", bash_command="sleep 5")
    task3 = BashOperator(task_id="tsk3", bash_command="exit 1", retries=0)
    task4 = BashOperator(task_id="tsk4", bash_command="sleep 5")
    task5 = BashOperator(task_id="tsk5", bash_command="sleep 5")
    task6 = BashOperator(task_id="tsk6", bash_command="sleep 5")
    task7 = BashOperator(task_id="tsk7", bash_command="sleep 5")
    task8 = BashOperator(task_id="tsk8", bash_command="sleep 5")
    task9 = BashOperator(task_id="tsk9", bash_command="sleep 5", trigger_rule="one_failed")

task1 >> task2
task3 >> task4
[task2, task4] >> task5 >> task6
task6 >> [task7, task8, task9]
