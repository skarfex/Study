
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, date
import logging
import requests

from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.hooks.postgres_hook import PostgresHook



def most_pop_loc_func():
    """
    The three most populated locations in Rick&Morty
    """
    api_url = 'https://rickandmortyapi.com/api/location'
    r = requests.get(api_url)
    if r.status_code == 200:
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("drop table students.public.r_lunin_ram_location", False)
        pg_hook.run("create table students.public.r_lunin_ram_location (id int, name varchar, type varchar, dimension varchar, resident_cnt int);", False)        

        result_json = (r.json()).get('results')
        resident_number = []

        for results in result_json:
            resident_number.append(len(results.get('residents')))

        for results in result_json:
            if len(results.get('residents')) in sorted(resident_number, reverse=True)[:3]:
                pg_hook.run(f"INSERT INTO public.r_lunin_ram_location (name, resident_cnt) VALUES ('{results.get('name')}', {len(results.get('residents'))});", False)
    else:
        logging.warning("HTTP STATUS {}".format(r.status_code))


DEFAULT_ARGS = {
    'owner': 'lunin',
    'email': ['i@rlunin.ru'],
    'email_on_failure': True,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': days_ago(2),   
    'trigger_rule':  'all_success'
}

with DAG(
    dag_id='R_lunin_dag_most_pop_loc',
    schedule_interval='10 13 * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['lunin']
) as dag:


    start = DummyOperator(
        task_id='start',
        trigger_rule='dummy'
    )


    most_pop_loc_task = PythonOperator(
        task_id='most_pop_loc_task',
        python_callable = most_pop_loc_func,
        trigger_rule='all_success'
    )


    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )


start >> most_pop_loc_task >> end