"""
Тестовый даг урок5
n-chernenko
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from n_chernenko_plugins.n_chernenko_ram_operator import NChernenkoRamOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n-chernenko',
    'poke_interval': 60
}

with DAG("n-chernenko_lesson5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chernenko']
         ) as dag:

    get_ram_operator = NChernenkoRamOperator(
        task_id='ram_operator',
        api_url='https://rickandmortyapi.com/api/location'
    )

    def write_to_db_func(**context):
        df = context['ti'].xcom_pull(task_ids='ram_operator', key='n-chernenko-ram')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(name='n_chernenko_ram_location', con=engine, if_exists='replace', index=False)
        engine.dispose()

    write_to_db = PythonOperator(
        python_callable=write_to_db_func,
        task_id='write_to_db',
        provide_context=True
    )

    get_ram_operator >> write_to_db
