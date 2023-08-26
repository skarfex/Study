from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.sensors import time_delta_sensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import requests

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'a-gurov-16'
}

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

def create_table():
    CREATE_TABLE_SQL_STATEMENT = """CREATE TABLE IF NOT EXISTS a_gurov_16_ram_location (
                                        id              int
                                        , name          varchar(100)
                                        , type          varchar(100)
                                        , dimension     varchar(100)
                                        , resident_cnt  int
                                    )"""
    pg_hook.run(CREATE_TABLE_SQL_STATEMENT, False)
    logging.info('SQL STATEMENT EXECUTION COMPLETED SUCCESSFULLY')


def find_top3_locations(**kwargs):
    ti = kwargs['ti']
    ans = requests.get('https://rickandmortyapi.com/api/location')
    location_list = []

    for item in ans.json()['results']:
        location_dict = {}
        location_dict['id'] = item['id']
        location_dict['name'] = item['name']
        location_dict['type'] = item['type']
        location_dict['dimension'] = item['dimension']
        location_dict['resident_cnt'] = len(item['residents'])
        location_list.append(location_dict)

    ti.xcom_push('location_list', sorted([item for item in location_list], key=lambda x: x['resident_cnt'], reverse=True)[:3])

    logging.info('TOP-3 LOCATIONS: {}'.format(sorted([item for item in location_list], key=lambda x: x['resident_cnt'], reverse=True)[:3]))


def write_data_to_gp(ti):
    location_list = ti.xcom_pull(task_ids='get_locations', key='location_list')
    logging.info('XCOM_PULL PARAMETER ACCEPTED: {}'.format(location_list))
    for item in location_list:
        pg_hook.run("INSERT INTO a_gurov_16_ram_location VALUES({}, '{}', '{}', '{}', {})".format(item['id'],
                                                                                                 item['name'],
                                                                                                 item['type'],
                                                                                                 item['dimension'],
                                                                                                 item['resident_cnt']), False)
    logging.info('INSERT STATEMENT COMPLETED SUCCESSFULLY')

dag = DAG(
    "a-gurov-16_lesson5_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS
)

t1 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag
)

t2 = PythonOperator(
    task_id='get_locations',
    python_callable=find_top3_locations,
    dag=dag
)

t3 = PythonOperator(
    task_id='write_data',
    python_callable=write_data_to_gp,
    dag=dag
)

t1 >> t2 >> t3