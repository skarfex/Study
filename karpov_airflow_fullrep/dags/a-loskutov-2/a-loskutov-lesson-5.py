"""
Lesson 5
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
import requests
import logging
import csv
import os

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-loskutov-2',
    'poke_interval': 600
}

with DAG(

    dag_id='a-loskutov-lesson-5',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
        tags=['Loskutov']
) as dag:

    ###############################################
    start = DummyOperator(task_id="start")
    ###############################################
    def get_page_count(api_url):
        r = requests.get(api_url)
        if r.status_code == 200:
            logging.info("SUCCESS")
            page_count = r.json().get('info').get('pages')
            logging.info(f'page_count = {page_count}')
            return page_count
        else:
            logging.warning("HTTP STATUS {}".format(r.status_code))
            raise AirflowException('Error in load page count')


    def get_top_locations(url='https://rickandmortyapi.com/api/location/?page={}'):
        list_ = []
        for i in range(1, get_page_count('https://rickandmortyapi.com/api/location/?page=1') + 1):
            r = requests.get(url.format(i))
            for res in r.json()['results']:
                list_.append((res['id'], res['name'], res['type'], res['dimension'], len(res['residents'])))
        list_ = sorted(list_, key=lambda x: x[-1], reverse=True)[:3]
        logging.info('Список: {}'.format(list_))
        with open('/tmp/loskutov.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            logging.info('writer!!!!!!!!!!!!!!!!!')
            for row in list_:
                writer.writerow(row)
                logging.info('-------------')
                logging.info(str(row)[1:-1])
                logging.info('-------------')

    get_top_loc = PythonOperator(task_id='get_top_loc', python_callable=get_top_locations)
    ###############################################
    def load_csv_to_gp():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY loskutov_ram_location FROM STDIN DELIMITER ','", '/tmp/loskutov.csv')

    load_csv_to_gp = PythonOperator(task_id='load_csv_to_gp', python_callable=load_csv_to_gp)
    ###############################################
    end = DummyOperator(task_id="end")

    start >> get_top_loc >> load_csv_to_gp >> end