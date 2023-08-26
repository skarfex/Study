from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from v_kosinov_9_plugins.top3_loc_operator import Kosinovtop3loc
#v3
DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'v-kosinov',
    'poke_interval': 600
}


with DAG("v-kosinov_ram_locations",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-kosinov']
         ) as dag:
    start = DummyOperator(task_id='start')

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=
        '''
        CREATE TABLE IF NOT EXISTS "v_kosinov_ram_location"
        (id INTEGER,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INTEGER
        )''',
        autocommit=True
    )

    xcom_push_ram_top_location = Kosinovtop3loc(
        task_id='xcom_push_ram_top_location'
    )

    load_top_locations_gp = PostgresOperator(
        task_id='load_top_locations_gp',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=[
            "TRUNCATE TABLE v_kosinov_ram_location",
            "INSERT INTO v_kosinov_ram_location VALUES {{ ti.xcom_pull(task_ids='xcom_push_ram_top_location') }}",
        ],
        autocommit=True
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> xcom_push_ram_top_location >> load_top_locations_gp >> end
