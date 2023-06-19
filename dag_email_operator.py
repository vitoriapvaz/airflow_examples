from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import statistics as sts

def data_cleaner():
    dataset = pd.read_csv("/home/vitoria/airflow/data/Arquivo.csv", sep=",") 
    dataset.columns = ["Id","Score","Estado","Genero","Idade","Patrimonio","Saldo","Produtos","TemCartCredito","Ativo","Salario","Saiu"]
    mediana = sts.median(dataset["Salario"])
    dataset["Salario"].fillna(mediana,inplace=True)
    dataset["Genero"].fillna("N", inplace=True)
    mediana = sts.median(dataset["Idade"])
    dataset.loc[(dataset["Idade"] < 0) | (dataset["Idade"] > 120), "Idade"] = mediana
    dataset.drop_duplicates(subset="Id", keep="first", inplace=True)

    dataset.to_csv("/home/vitoria/airflow/data/Arquivo_Clean.csv", sep=";", index=False)

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
    dag_id="dag_email_operator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["envia-email", "email-operator"]
) as dag:

    task1 = PythonOperator(task_id="tsk1", python_callable=data_cleaner, email_on_failure=True)
    task2 = BashOperator(task_id="tsk2",bash_command="sleep 3")
    send_email = EmailOperator(task_id="send_email",
                    to="maria.vaz@123milhas.com.br",
                    subject="Airflow Error",
                    html_content="""<h3>Ocorreu um erro na Dag. </h3>
                                    <p>Dag: dag_email_operator </p>  
                                """, trigger_rule="one_failed")

task1 >> [task2, send_email]
