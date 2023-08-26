from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from a_kiselev_9_plugins.a_kiselev_ram_loc import KiselevRamOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-kiselev',
    'poke_interval': 600}

with DAG('a-kiselev_task5',
     schedule_interval=None,
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-kiselev']) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        '''
        CREATE TABLE IF NOT EXISTS "a_kiselev_ram_location"
        (id INT,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INT       
        )''',
        autocommit=True
    )

    residents_ram_location = KiselevRamOperator(
        task_id='residents_ram_location'
    )

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE a_kiselev_ram_location",
            "INSERT INTO a_kiselev_ram_location VALUES {{ ti.xcom_pull(task_ids='residents_ram_location') }}",
        ],
        autocommit=True
    )

    create_table >> residents_ram_location >> load_data



