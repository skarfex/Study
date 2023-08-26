"""
Разработка своих плагинов. Практическое задание.
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from m_kochenjuk_plugins.kochenjuk_ram import KochenjukLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-kochenjuk-10'
}

GP_TABLE_NAME = 'kochenjuk_ram_location'
GP_CONN_ID = 'conn_greenplum_write'

with DAG('m-kochenjuk-10_ram',
         schedule_interval='0 0 * * *',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-kochenjuk-10']
         ) as dag:

    start = DummyOperator(task_id='start')
    
    create_table = PostgresOperator(
        task_id='create_table',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {GP_TABLE_NAME} (
                id  SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT4 NOT NULL);
        """,
        postgres_conn_id=GP_CONN_ID
    )
    delete_rows = PostgresOperator(
        task_id='delete_rows',
        postgres_conn_id=GP_CONN_ID,
        sql=f"DELETE FROM {GP_TABLE_NAME};",
        autocommit=True
    )

    load_ram_locations = KochenjukLocationOperator(
        task_id='load_ram_locations',
        pages_count=2,
        gp_table_name=GP_TABLE_NAME,
        gp_conn_id=GP_CONN_ID
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> delete_rows >> load_ram_locations >> end