from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook


def printval():
    print('Simple string')


with DAG(
        dag_id="d-gutyrchik-16",
        start_date=datetime(2023, 1, 8),
        schedule_interval='@daily',
        max_active_runs=1,
        catchup=False,
        tags=["dgutyrchik"]
) as dag:
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=printval,
        dag=dag)

    python_task
