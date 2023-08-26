"""
Вывод строки из GreenPlum
"""
from airflow import DAG
import psycopg2.extras
from airflow.utils.dates import days_ago

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from p_ovchinnikov_20_plugins.p_ovchinnikov_20_api_operator import ApiTopLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'p-ovchinnikov-20',
    'poke_interval': 600
}

with DAG("p-ovchinnikov-20-task-5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p-ovchinnikov-20']
) as dag:

    def write_locations_to_gp_func(ti, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute('DROP TABLE IF EXISTS p_ovchinnikov_20_ram_location;')
        cursor.execute('''
            CREATE TABLE p_ovchinnikov_20_ram_location (
                id int not null, 
                name varchar not null, 
                type varchar not null,
                dimension varchar not null, 
                resident_cnt int not null
            )
        ''')

        data = ti.xcom_pull(task_ids='get_top_locations')
        psycopg2.extras.execute_values(
            cursor,
            'INSERT INTO p_ovchinnikov_20_ram_location VALUES %s',
            data,
            template='(%(id)s, %(name)s, %(type)s, %(dimension)s, %(resident_cnt)s)',
            page_size=10
        )
        conn.commit()

    get_top_locations = ApiTopLocationOperator(
        task_id='get_top_locations'
    )

    write_locations_to_gp = PythonOperator(
        task_id='write_locations_to_gp',
        python_callable=write_locations_to_gp_func
    )

    get_top_locations >> write_locations_to_gp



