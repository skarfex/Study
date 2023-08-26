from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from v_korolko_plugins.v_korolko_top_locations import VKorolkoTop3LocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-korolko',
    'poke_interval': 600,
    'retry_delay': timedelta(seconds=30),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=1)
}

dag=DAG("v-korolko_dag_top3loc",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    tags=['v-korolko_dag_top3loc']
)
truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS public.v_korolko_ram_location
            (
                id           INTEGER PRIMARY KEY,
                name         VARCHAR(256),
                type         VARCHAR(256),
                dimension    VARCHAR(256),
                resident_cnt INTEGER
            )
                DISTRIBUTED BY (id);''',
            '''TRUNCATE TABLE v_korolko_ram_location;'''
        ],
        autocommit=True,
        dag=dag
)

get_top3_locations = VKorolkoTop3LocationOperator(
        task_id='top3_locations_Rick_and_Morty',
        dag=dag
    )

truncate_table>>get_top3_locations

