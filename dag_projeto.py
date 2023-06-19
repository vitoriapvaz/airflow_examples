from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os


def process_file(**context):
    with open(Variable.get("path_file_turbine")) as f:
        data = json.load(f)
        context["ti"].xcom_push(key="idtemp", value=data["idtemp"])
        context["ti"].xcom_push(key="powerfactor", value=data["powerfactor"])
        context["ti"].xcom_push(key="hydraulicpressure",
                                value=data["hydraulicpressure"])
        context["ti"].xcom_push(key="temperature", value=data["temperature"])
        context["ti"].xcom_push(key="timestamp", value=data["timestamp"])

    os.remove(Variable.get("path_file_turbine"))


def avalia_temp(**context):
    number = float(context["ti"].xcom_pull(task_ids="get_data", key="temperature"))
    if number >= 24:
        return "group_check_temp.send_email_alert"
    else:
        return "group_check_temp.send_email_normal"


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
    dag_id="dag_projeto_turbina",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    default_view="graph",
    doc_md="## Dag para registrar dados de turbina eólica"
) as dag:

    group_check_temp = TaskGroup("group_check_temp")
    group_database = TaskGroup("group_database")

    file_sensor_task = FileSensor(
        task_id="file_sensor_task",
        filepath=Variable.get("path_file_turbine"),
        fs_conn_id="connection_file_turbine",
        poke_interval=5,
        timeout=60)

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=process_file,
        provide_context=True)

    create_table = SqliteOperator(task_id="create_table",
                                  sqlite_conn_id="sqlite_default",
                                  sql="""create table if not exists log (
                                        campo_leitura varchar
                                    )
                                    """,
                                  task_group=group_database)

    insert_data = SqliteOperator(task_id="insert_data",
                                 sqlite_conn_id="sqlite_default",
                                 sql=f"""INSERT INTO log (campo_leitura)
                                        VALUES ('leitura realizada')
                                    """,
                                 task_group=group_database)

    query_data = SqliteOperator(task_id="query_data",
                                sqlite_conn_id="sqlite_default",
                                sql="SELECT * FROM table_teste",
                                task_group=group_database)

    print_result_task = PythonOperator(task_id="print_result",
                                       python_callable=print_result,
                                       task_group=group_database)

    send_email_alert = EmailOperator(task_id="send_email_alert",
                                     to="maria.vaz@123milhas.com.br",
                                     subject="Airlfow alert",
                                     html_content="""<h3>Alerta de Temperatura. </h3>
                                                    <p> Dag: windturbine </p>
                                                """,
                                     task_group=group_check_temp)

    send_email_normal = EmailOperator(task_id="send_email_normal",
                                      to="maria.vaz@123milhas.com.br",
                                      subject="Airlfow advise",
                                      html_content="""<h3>Temperaturas normais. </h3>
                                                    <p> Dag: windturbine </p>
                                                """,
                                      task_group=group_check_temp)

    check_temp_branc = BranchPythonOperator(task_id="check_temp_branc",
                                            python_callable=avalia_temp,
                                            provide_context=True,
                                            task_group=group_check_temp)

with group_check_temp:
    check_temp_branc >> [send_email_alert, send_email_normal]

with group_database:
    create_table >> insert_data >> query_data >> print_result_task

file_sensor_task >> get_data
get_data >> group_check_temp
get_data >> group_database
