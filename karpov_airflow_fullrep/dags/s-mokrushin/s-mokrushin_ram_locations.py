from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from s_mokrushin_plugins.s_mokrushin_ram_locations_operator import SmokrushinRAMAPILocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 's-mokrushin',
    'poke_interval': 600
}

with DAG("s-mokrushin_ram_locations",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-mokrushin']
         ) as dag:

    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=
            '''
            CREATE TABLE IF NOT EXISTS "s_mokrushin_ram_location"
            (id INTEGER,
            name VARCHAR,
            type VARCHAR,
            dimension VARCHAR,
            resident_cnt INTEGER
            )''',
        autocommit=True
    )

    xcom_push_ram_top_location = SmokrushinRAMAPILocationsOperator(
        task_id='xcom_push_ram_top_location'
    )

    load_top_locations_gp = PostgresOperator(
        task_id='load_top_locations_gp',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=[
            "TRUNCATE TABLE s_mokrushin_ram_location",
            "INSERT INTO s_mokrushin_ram_location VALUES {{ ti.xcom_pull(task_ids='xcom_push_ram_top_location') }}",
        ],
        autocommit=True
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> xcom_push_ram_top_location >> load_top_locations_gp >> end
