"""

RAM lesson_5

Создаем таблицу в GP, добавляем в нее топ3 локации по кол-ву резидентов. Данные берем из API

"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from n_kislitsyn_plugins.n_kislitsyn_rick_and_morty_locations import KislitsynRickAndMortyOperator


TABLE_NAME = 'n_kislitsyn_ram_location'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'n-kislitsyn',
    'poke_interval': 600
}

dag = DAG("n-kislitsyn_ram_dag",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['n-kislitsyn']
        )

dummy_start = DummyOperator(task_id = 'Start_task', dag = dag)

create_table = PostgresOperator(
    task_id = 'Create_table',
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

clear_table = PostgresOperator(
    task_id = "Clear_table",
    postgres_conn_id='conn_greenplum_write',
    sql = f"TRUNCATE TABLE {TABLE_NAME};",
    autocommit=True,
    dag=dag
)

ram_execute = KislitsynRickAndMortyOperator(
    task_id='ram_api_top3_locations',
    conn_id='conn_greenplum_write',
    table_name=TABLE_NAME,
    dag=dag
)

dummy_start >> create_table >> clear_table >> ram_execute



