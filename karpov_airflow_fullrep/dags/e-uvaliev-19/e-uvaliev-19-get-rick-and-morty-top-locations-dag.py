from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from e_uvaliev_19_plugins.eUvaliev19RickandMortyTopLocations import eUvaliev19RamTop3LocationOperator

DEFAULT_ARGS: dict = {
    'start_date': days_ago(2),
    'owner': 'e-uvaliev-19',
    'poke_interval': 600,
}

def create_table():
    query1 = '''
        CREATE TABLE IF NOT EXISTS public.e_uvaliev_19_ram_location (
                id INT PRIMARY KEY,
                name VARCHAR,
                type VARCHAR,
                dimension VARCHAR,
                resident_cnt INT
                );
    '''
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    pg_hook.run(query1)

def truncate_table():
    query2 = '''
        TRUNCATE TABLE public.e_uvaliev_19_ram_location;
    '''
    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
    pg_hook.run(query2)

with DAG("e-uvaliev-19-get-rick-and-morty-top-locations-dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-uvaliev-19']
    ) as dag:

    create_table_task = PythonOperator(
        task_id='create_table_task',
        python_callable=create_table
    )

    truncate_table_task = PythonOperator(
        task_id='truncate_table_task',
        python_callable=truncate_table
    )

    insert_RnM_top_locations_from_api = eUvaliev19RamTop3LocationOperator(
        task_id='insert_RnM_top_locations_from_api'
    )

    create_table_task >> truncate_table_task >>  insert_RnM_top_locations_from_api