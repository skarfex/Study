from datetime import datetime
import logging
from psycopg2.sql import SQL, Identifier

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


from a_orlov_plugins import RickandmortyTopLocsOperator


logging.basicConfig(level=logging.INFO)


DEFAULT_ARGS = {
    'owner': 'Lexx Orlov',
    'start_date': datetime(2022, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': None,
    'depends_on_past': False
}


@dag(dag_id='ram_dag',
     description='Rick and Morty dag',
     default_args=DEFAULT_ARGS,
     schedule_interval='@once',
     max_active_runs=1,
     tags=['lexxo'])
def dag_ram():


    def connect_to_db(conn_name):
        hook = PostgresHook(conn_name)
        conn = hook.get_conn()
        cursor = conn.cursor('ram_cursor')
        return conn, cursor

    def create_ram_table(conn, cursor, tablename):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS %s(
            id              int primary key,
            name            text,
            type            text,
            dimension       text,
            resident_cnt    int
            );
        """, (tablename,))
        conn.commit()

    start = DummyOperator(task_id='start')

    @task()
    def create_table(name, **kwargs):
        cursor = connect_to_db('conn_greenplum_write')
        create_ram_table(cursor, name)
        kwargs['ti'].xcom_push(key='table_name', value=name)

    create_table_psql = PostgresOperator(
        task_id='create_table_ps_op',
        postgres_conn_id = 'conn_greenplum_write',
        sql="""
            CREATE TABLE IF NOT EXISTS public.a_orlov_ram_location (
            id              int primary key,
            name            text,
            type            text,
            dimension       text,
            resident_cnt    int
            );
            """)

    get_top_locs = RickandmortyTopLocsOperator(task_id='get_top3_locs', top_locations=3)


    @task()
    def load_entries(**kwargs):
        # conn, cursor = connect_to_db('conn_greenplum_write')
        hook = PostgresHook('conn_greenplum_write')
        conn = hook.get_conn()
        cursor = conn.cursor()
        df = kwargs['ti'].xcom_pull(task_ids='get_top3_locs', key='return_value')
        for i in df.iterrows():
            data = i[1]
            values_string = tuple(data.values)
            logging.info(f'Inserting values: {values_string}')
            cursor.execute(f"INSERT INTO public.a_orlov_ram_location VALUES {values_string};")
            conn.commit()


    start >> create_table_psql >> get_top_locs >> load_entries()


insert_top_locs_into_greenplum = dag_ram()

