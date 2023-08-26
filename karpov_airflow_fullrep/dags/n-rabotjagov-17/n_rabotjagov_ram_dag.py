from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from n_rabotjagov_17_plugins.n_rabotjagov_ram_operator import RaboRAMLocOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'n-rabotjagov-17',
    'poke_interval': 600
}

with DAG(
         dag_id="n_rabotjagov_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-rabotjagov-17']
         ) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=
        '''
        CREATE TABLE IF NOT EXISTS "n_rabotjagov_17_ram_location"
        (id INT,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INT       
        )''',
        autocommit=True
    )

    xcom_push_ram_loc = RaboRAMLocOperator(
        task_id='xcom_push_ram_loc'
    )

    load_loc_gp = PostgresOperator(
        task_id='load_loc_gp',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=[
            "TRUNCATE TABLE n_rabotjagov_17_ram_location",
            "INSERT INTO n_rabotjagov_17_ram_location VALUES {{ ti.xcom_pull(task_ids='xcom_push_ram_loc') }}",
        ],
        autocommit=True
    )

    start >> create_table >> xcom_push_ram_loc >> load_loc_gp >> finish
