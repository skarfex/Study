from airflow.providers.postgres.operators.postgres import PostgresOperator
from v_pavlovskij_12_plugins.v_pavlovskij_12_location_operator import VPavlovskijLocationOperator
from v_pavlovskij_12_plugins.v_pavlovskij_12_insert_row_operator import VPavlovskijInsertRowOperator
from airflow import DAG
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-pavlovskij-12'
}

with DAG(
    'v-pavlovskij-12-lesson-5-1',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-pavlovskij-12']
) as dag: 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            CREATE TABLE IF NOT EXISTS public.v_pavlovskij_12_ram_location
            (
                id integer,
                name varchar,
                type varchar,
                dimension varchar,
                resident_cnt integer
            )
        '''
    )

    get_locations = VPavlovskijLocationOperator(
        task_id='get_locations'
    )

    insert_rows = VPavlovskijInsertRowOperator(
        task_id='insert_rows'
    )

    create_table >> get_locations >> insert_rows