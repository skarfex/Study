from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from ale_suchilin_plugins.ale_suchilin_operator import ASTopLocationsOperator

DEFAULT_ARGS = {
    'owner': 'suchilin',
    'email': ['a.souchilin@yandex.ru'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 6, 11),
    'poke_interval': 600, #rrr
    'trigger_rule':  'one_success'
}

with DAG("ale_suchilin_operator_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ale-suchilin']
) as dag:
    
    start = DummyOperator(
        task_id="start"
    )
    
    print_locations = ASTopLocationsOperator(
        task_id='print_locations'
    )

    table_creation = PostgresOperator(
    task_id='table_creation',
    postgres_conn_id='conn_greenplum_write',
    sql=[
        '''
        CREATE TABLE IF NOT EXISTS "ale-suchilin_ram_location"
        (
             id           INT PRIMARY KEY,
             name         VARCHAR(300),
             type         VARCHAR(300),
             dimension    VARCHAR(300),
             resident_cnt INT
        )
            DISTRIBUTED BY (id);''',
        '''TRUNCATE TABLE "ale_suchilin_ram_location";'''
        ],
    autocommit=True
    )

    end = DummyOperator(
        task_id="end"
    )
    start >> table_creation >> print_locations >> end
