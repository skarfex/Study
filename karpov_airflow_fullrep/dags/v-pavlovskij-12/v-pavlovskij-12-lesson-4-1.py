from airflow import DAG
from airflow.utils.dates import days_ago
import pandas as pd
import logging
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-pavlovskij-12',
    'poke_interval': 600
}

with DAG(
    'v-pavlovskij-12-lesson-4-1',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-pavlovskij-12']
) as dag: 

    def check_weekday(**kwargs):
        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        kwargs['ti'].xcom_push(value=weekday, key='weekday')
        return weekday != 7  

    weekday_operator = ShortCircuitOperator(
        task_id='weekday_operator',
        python_callable=check_weekday,
        provide_context=True
    )

    def get_headings(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        with pg_hook.get_conn() as conn:
            df = pd.read_sql(
                'SELECT heading FROM articles WHERE id = {weekday}'.format(
                    weekday=kwargs['ti'].xcom_pull(task_ids='weekday_operator', key='weekday')
                ),
                con=conn
            )
        kwargs['ti'].xcom_push(value=df['heading'].tolist(), key='heading')
    
    heading_task = PythonOperator(
        task_id='heading_task',
        python_callable=get_headings,
        provide_context=True
    )

    weekday_operator >> heading_task
    


