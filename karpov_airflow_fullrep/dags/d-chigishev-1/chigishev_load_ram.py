from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from d_chigishev_1_plugins.chigishev_ram_operator import TopResidentsLocationsRickAndMorty

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'd-chigishev-1',
    'poke_interval': 600
}

with DAG("d-chigishev-1_ram",
         schedule_interval=None,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-chigishev-1', 'rick_and_morty']
         ) as dag:

    start_task = DummyOperator(task_id='start_task')

    create_or_truncate_table = PostgresOperator(
        task_id='create_or_truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "chigishev_ram_location"
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE "chigishev_ram_location";'''
        ],
        autocommit=True
    )

    top_residents_locations_rick_and_morty = TopResidentsLocationsRickAndMorty(task_id='top_residents_locations_rick_and_morty')

    end_process = DummyOperator(task_id='end_process')

    start_task >> create_or_truncate_table >> top_residents_locations_rick_and_morty >> end_process
