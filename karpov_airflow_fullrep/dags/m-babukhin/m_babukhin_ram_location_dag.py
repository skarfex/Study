"""
dag for saving top-3 locations from Rick and Morty into greenplum table
"""

import logging
import pendulum

from m_babukhin_plugins.m_babukhin_custom_operator import BabukhinLocationDiscoveryOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'owner': 'm-babukhin',
    'start_date': pendulum.datetime(2023, 2, 18, tz='utc'),
    'poke_interval': 600
}

dag = DAG("m_babukhin_ram_location",
          schedule_interval='@once',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['m-babukhin']
          )

locations_to_csv = BabukhinLocationDiscoveryOperator(
    task_id='locations_to_csv',
    dag=dag
)


def load_csv_to_gp_func():
    """
    function, that create specified table if not exists and insert data from csv_file
    """
    postgres_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    table_name = "m_babukhin_ram_location"
    # schema_name = "public"

    table_exists = postgres_hook.get_first(f"SELECT EXISTS( \
                SELECT 1 FROM information_schema.tables WHERE table_name='{table_name}')")[0]
    if not table_exists:
        postgres_hook.run(
            f"CREATE TABLE {table_name} (id varchar, name varchar, type varchar, dimension varchar, resident_cnt int)")
        logging.info('Table was created')
    else:
        postgres_hook.run(f"DELETE FROM {table_name}")
        logging.info('Table was cleared')

    postgres_hook.copy_expert(
        "COPY m_babukhin_ram_location FROM STDIN DELIMITER ','", '/tmp/location_data.csv')
    logging.info('Data was inserted')


load_csv_to_greenplum = PythonOperator(
    task_id='load_csv_to_greenplum',
    python_callable=load_csv_to_gp_func,
    dag=dag
)

locations_to_csv >> load_csv_to_greenplum
