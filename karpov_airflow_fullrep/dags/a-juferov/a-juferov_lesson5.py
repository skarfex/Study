"""
Gets top 3 locations by residents of Rick and Morty series and stores to DB
"""
import psycopg2.extras
import pendulum

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from a_juferov_plugins.a_juferov_ram_top_residents_locations_operator import AJuferovRamTopResidentsLocationsOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 6, 10, tz="UTC"),
    'owner': 'a-juferov'
}


def write_top_locations_to_db_func(ti, **kwargs):
    """ Stores data to DB """
    data = ti.xcom_pull(task_ids='get_top_3_locations')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS a_juferov_ram_location (
            id int primary key not null,
            name varchar not null,
            type varchar not null,
            dimension varchar not null,
            resident_cnt int not null default 0
        )
    ''')

    cursor.execute('TRUNCATE TABLE a_juferov_ram_location')

    psycopg2.extras.execute_values(
        cursor,
        'INSERT INTO a_juferov_ram_location VALUES %s',
        data,
        template='(%(id)s, %(name)s, %(type)s, %(dimension)s, %(resident_cnt)s)',
        page_size=500
    )

    conn.commit()


with DAG("a-juferov_lesson5",
     schedule_interval=None,
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-juferov']) as dag:

    get_top_3_locations = AJuferovRamTopResidentsLocationsOperator(
        task_id='get_top_3_locations',
        top_size=3
    )

    write_top_locations_to_db = PythonOperator(
        task_id='write_top_locations_to_db',
        python_callable=write_top_locations_to_db_func
    )

    get_top_3_locations >> write_top_locations_to_db
