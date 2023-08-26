from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s_shirokov_21_plugins.s_shirokov_21_ram_top_loc import RamLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-shirokov-21',
}

with DAG("s-shirokov-21_ram_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-shirokov-21']
         ) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS "s_shirokov_21_ram_location" (
                id INT PRIMARY KEY,
                name VARCHAR NOT NULL,
                type VARCHAR NOT NULL,
                dimension VARCHAR NOT NULL,
                resident_cnt INT NOT NULL);
        """,
        autocommit=True
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f'TRUNCATE TABLE "s_shirokov_21_ram_location";',
        autocommit=True,
    )

    top_loc_add = RamLocationOperator(
        task_id='top_loc_add',
    )

    create_table >> truncate_table >> top_loc_add
