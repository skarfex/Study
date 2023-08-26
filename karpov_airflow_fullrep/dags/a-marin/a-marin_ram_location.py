from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from a_marin_plugins.a_marin_ram_operator import AmarinRAMLocOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-marin',
    'poke_interval': 600
}

with DAG(
         dag_id="a-marin_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-marin']
         ) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=
        '''
        CREATE TABLE IF NOT EXISTS "a_marin_ram_location"
        (id INT,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INT       
        )''',
        autocommit=True
    )

    xcom_push_ram_loc = AmarinRAMLocOperator(
        task_id='xcom_push_ram_loc'
    )

    load_loc_gp = PostgresOperator(
        task_id='load_loc_gp',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=[
            "TRUNCATE TABLE a_marin_ram_location",
            "INSERT INTO a_marin_ram_location VALUES {{ ti.xcom_pull(task_ids='xcom_push_ram_loc') }}",
        ],
        autocommit=True
    )

    start >> create_table >> xcom_push_ram_loc >> load_loc_gp >> finish
