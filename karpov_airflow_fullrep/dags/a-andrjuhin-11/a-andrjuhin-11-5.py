import logging
import time
from datetime import datetime, timedelta
from airflow import DAG
import airflow
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

table_name = 'a_andrjuhin_11_ram_location'

def task_print():
    logging.info("a-andrjuhin - task is being executed")
    time.sleep(10)

def log_on_failure(context):
    logging.warning("a-andrjuhin - task failed")

def log_on_success(context):
    logging.info("a-andrjuhin - task finished")

def log_on_retry(context):
    logging.info("a-andrjuhin - task retry")

def log_on_sla_miss(context):
    logging.info("a-andrjuhin - execution time bigger than usual")


DEFAULT_ARGS = {
    'owner': 'a-andrjuhin-11',
    'depends_on_past': True,
    'wait_for_downstream': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2022, 8, 30),
    'sla': timedelta(seconds=300),
    'execution_timeout': timedelta(seconds=1000),
    'on_failure_callback': log_on_failure,
    'on_success_callback': log_on_success,
    'on_retry_callback': log_on_retry,
    'sla_miss_callback': log_on_sla_miss,
    'trigger_rule': 'all_success'
    
}

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')

dag = DAG("a-andrjuhin-11-rickandmorty",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    tags=['a-andrjuhin-11', 'андрюхин']
)

def create_table_def():
    sql_string = f"CREATE TABLE IF NOT EXISTS {table_name}\n("
    sql_string += "id integer,\n"
    sql_string += "name varchar,\n"
    sql_string += "type varchar,\n"
    sql_string += "dimension varchar,\n"
    sql_string += "resident_cnt int);" 
    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_string)

def get_data_from_api_def():
    import requests
    import json
    # with open("airflow/dags/api_response_rickandmorty.json") as response:
    #     req = json.loads(response.read())
    #     ans = req["results"]

    req = requests.get("https://rickandmortyapi.com/api/location")
    if req.status_code == 200:
        ans = json.loads(req.text)["results"]
        top = sorted(ans, key=(lambda place: len(place["residents"])), reverse=True)[:3]
        sql_string1 = f'DELETE FROM {table_name}'
        sql_string2 = f'INSERT INTO {table_name} (id, name, type, dimension, resident_cnt) VALUES' 
        for rec in top:
            sql_string2 += f"\n ({rec['id']}, '{rec['name']}', '{rec['type']}', '{rec['dimension']}', {len(rec['residents'])}),"
        sql_string2 = sql_string2[:-1] + ";" 
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_string1)
            cursor.execute(sql_string2)

create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_def,
    provide_context=True,
    dag=dag
)

get_data_from_api = PythonOperator(
    task_id='get_data_from_api',
    python_callable=get_data_from_api_def,
    provide_context=True,
    dag=dag
)

create_table >> get_data_from_api

if __name__ == "__main__":
    get_data_from_api()

