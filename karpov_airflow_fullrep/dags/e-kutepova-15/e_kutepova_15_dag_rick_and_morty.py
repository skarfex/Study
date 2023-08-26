"""
Создаем или чистим таблицу в GP, идем в API за инфой по локациям и резидентам, складываем в таблицу в GP
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from e_kutepova_15_plugins.e_kutepova_15_rick_and_morty_locations import GetTopLocations

TABLE_NAME = 'e_kutepova_15_ram_location'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-kutepova-15',
    'poke_interval': 600
}

dag = DAG("e-kutepova-15.rick_and_morty",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['e-kutepova-15']
        )

dummy_start = DummyOperator(task_id='start_task', dag=dag)

dummy_end = DummyOperator(task_id='end_task', dag=dag)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL4 PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            resident_cnt INT4 NOT NULL);
    """,
    autocommit=True,
    dag=dag
)

truncate_table = PostgresOperator(
    task_id='truncate_table',
    postgres_conn_id='conn_greenplum_write',
    sql=f"TRUNCATE TABLE {TABLE_NAME};",
    autocommit=True,
    dag=dag
)

api_get_locations = GetTopLocations(
    task_id='api_get_locations',
    conn_id='conn_greenplum_write',
    table_name=TABLE_NAME,
    dag=dag
)

dummy_start >> create_table >> truncate_table >> api_get_locations >> dummy_end