from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from a_kapranov_plugins.a_kapranov_top_residents_ram import KapranovRamResidentsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-kapranov',
    'poke_interval': 600}

with DAG('a-kapranov_homework5',
     schedule_interval=None,
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-kapranov']) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        '''
        CREATE TABLE IF NOT EXISTS "a_kapranov_ram_location"
        (id INT,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INT       
        )''',
        autocommit=True
    )

    residents_ram_location = KapranovRamResidentsOperator(
        task_id='residents_ram_location'
    )

    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE a_kapranov_ram_location",
            "INSERT INTO a_kapranov_ram_location VALUES {{ ti.xcom_pull(task_ids='residents_ram_location') }}",
        ],
        autocommit=True
    )

    create_table >> residents_ram_location >> load_data



