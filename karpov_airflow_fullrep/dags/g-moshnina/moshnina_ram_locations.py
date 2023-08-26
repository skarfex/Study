"""
Передает через XCom значение поля heading из строки с id, равным дню недели ds, из таблицы articles karpovcourses greenplum database
Второй таск получает эту строку и пишет её в логи
"""
from airflow import DAG
import logging
import datetime as dt

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from moshnina_plugins.moshnina_ram_locations_operator import MoshninaRamLocationsOperator

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022,12,10),
    'owner': 'g-moshnina',
    'poke_interval': 600
}

with DAG("moshnina_ram_locations",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['g-moshnina']
) as dag:

    def create_gp_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("CREATE TABLE IF NOT EXISTS public.g_moshnina_top_ram_locations (\
                     id integer primary key, location_name varchar(30), \
                     dimension varchar(30), residents_number integer);"
                    , False)
        logging.info('Table is ready')
               
    create_gp_table = PythonOperator(
        task_id='create_gp_table',
        python_callable=create_gp_table
    )

    ram_top_locations = MoshninaRamLocationsOperator(
        task_id='ram_top_locations'
    )

    create_gp_table >> ram_top_locations
