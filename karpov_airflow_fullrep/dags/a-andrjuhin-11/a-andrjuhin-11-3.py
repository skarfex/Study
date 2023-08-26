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
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,15),
    'sla': timedelta(seconds=300),
    'execution_timeout': timedelta(seconds=1000),
    'on_failure_callback': log_on_failure,
    'on_success_callback': log_on_success,
    'on_retry_callback': log_on_retry,
    'sla_miss_callback': log_on_sla_miss,
    'trigger_rule': 'all_success'
}

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')

dag = DAG("a-andrjuhin-11-dag3",
    schedule_interval='0 12 * * MON,TUE,WED,THU,FRI,SAT',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=True,
    tags=['a-andrjuhin-11', 'андрюхин']
)

def get_string_for_today_func(**kwargs):
    this_date = kwargs['ds']
    day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
    with pg_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_res = cursor.fetchall()
        result = query_res[0][0]
    logging.info(datetime.strptime(this_date, '%Y-%m-%d'))
    logging.info(str(day_of_week) + " - " + result)



get_string_for_today = PythonOperator(
    task_id='andrjuhin_task',
    python_callable=get_string_for_today_func,
    provide_context=True,
    dag=dag
)
