
"""
Даг для урока 5 (Rick and Morty top 3 locations).
"""


from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from m_stratonnikov_plugins.ram_location_operator import RamLocationOperator

from sqlalchemy import create_engine

TABLE_NAME = 'public.m_stratonnikov_ram_location'
DEF_CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-stratonnikov',
    'retries': 3
}

with DAG(
    "m-stratonnikov_ram_dag",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-stratonnikov'],
) as dag:

    get_top3 = RamLocationOperator(
        task_id='get_top3',
        url='https://rickandmortyapi.com/api/location'
    )

    def write_top3_f(**context):
        top_df = context['ti'].xcom_pull(key='m_stratonnikov_ram')
        pg_hook = PostgresHook(postgres_conn_id=DEF_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        top_df.to_sql(name=TABLE_NAME, con=engine, if_exists='replace') # creates table, if id doesn't exist
        pg_hook.run('ALTER TABLE public.m_stratonnikov_ram_location DROP COLUMN index;')
        engine.dispose()



    write_to_db = PythonOperator(
        task_id='write_to_db',
        python_callable=write_top3_f,
    )

get_top3 >> write_to_db

dag.doc_md = __doc__
get_top3.doc_md = """Gets top-3 locations with residents"""
write_to_db.md = """Puts top-3 locations to db"""
