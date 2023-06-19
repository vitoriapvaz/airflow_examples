from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import statistics as sts

def data_cleaner():
    dataset = pd.read_csv("/home/vitoria/airflow/data/Clients.csv", sep=",") 
    dataset.columns = ["Id","Score","Estado","Genero","Idade","Patrimonio","Saldo","Produtos","TemCartCredito","Ativo","Salario","Saiu"]
    mediana = sts.median(dataset["Salario"])
    dataset["Salario"].fillna(mediana,inplace=True)
    dataset["Genero"].fillna("N", inplace=True)
    mediana = sts.median(dataset["Idade"])
    dataset.loc[(dataset["Idade"] < 0) | (dataset["Idade"] > 120), "Idade"] = mediana
    dataset.drop_duplicates(subset="Id", keep="first", inplace=True)

    dataset.to_csv("/home/vitoria/airflow/data/Clients_Clean.csv", sep=";", index=False)

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
    dag_id="dag_python_operator",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
   
   task1 = PythonOperator(task_id="tsk1", python_callable=data_cleaner)

task1