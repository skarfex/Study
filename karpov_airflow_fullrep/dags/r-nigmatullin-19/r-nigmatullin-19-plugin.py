from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from r_nigmatullin_19_plugins.r_nigmatullin_19_ram_top_location_operator import RamLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'r-nigmatullin-19',
}

with DAG("r-nigmatullin-19_plugin_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['r-nigmatullin-19']
         ) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS "r-nigmatullin-19_ram_location" (
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
        sql=f'TRUNCATE TABLE "r-nigmatullin-19_ram_location";',
        autocommit=True,
    )

    top_location_append = RamLocationOperator(
        task_id='top_location_insert_to_table',
    )

    create_table >> truncate_table >> top_location_append
