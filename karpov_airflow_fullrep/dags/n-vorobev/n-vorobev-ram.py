"""
API = https://rickandmortyapi.com/documentation/#location
"""

from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from n_vorobev_plugins.n_vorobev_ram_operator import (
    VorobevNRamOperator
)

TABLE_NAME = 'n_vorobev_ram_location'

DEFAULT_ARGS = {
    'owner': 'n-vorobev',
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(year=2022, month=9, day=12, hour=0, minute=0),
    'end_date': datetime(year=2022, month=9, day=12, hour=0, minute=0),
    'provide_context': True
}

with DAG("n-vorobev-ram",
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * * *",
    max_active_runs=1,
    tags=['n-vorobev-3']
) as dag:

    get_top3 = VorobevNRamOperator(
        task_id='top3',
        api_url='https://rickandmortyapi.com/api/location'
    )

    def write_to_db_func(**context):
        df = context['ti'].xcom_pull(task_ids='top3', key='n-vorobev-ram')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace')  # creates table, if id doesn't exist
        engine.dispose()

    write_to_db = PythonOperator(
        python_callable=write_to_db_func,
        task_id='write_to_db',
        provide_context=True
    )

    get_top3 >> write_to_db
