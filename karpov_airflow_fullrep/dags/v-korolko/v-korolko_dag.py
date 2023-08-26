"""
Тренировочный даг
выбираем данные из Greenplum в лог
"""
from airflow import DAG
import logging
from datetime import datetime, timedelta
#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.python import BranchPythonOperator
#from airflow.sensors.time_delta import TimeDeltaSensor
#import random
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    #'depends_on_past': True,
    #'wait_for_downstream': True,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-korolko',
    'poke_interval': 600
}

dag=DAG("v-korolko_dag",
    description='example for course of data engineering: ETL, module 3, lesson 3',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    #max_active_runs=1,
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['v-korolko_dag']
)

def import_datagp_to_log_func(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    this_date = kwargs['ds']
    day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
    query_res = cursor.fetchone()[0]  #полный результат
    #result = query_res()[0]
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info('--------')
    logging.info(f'Weekday: {day_of_week}')
    logging.info(f'Return value: {query_res}')
    logging.info('--------')

import_datagp_to_log = PythonOperator(
    task_id='import_datagp_to_log',
    python_callable=import_datagp_to_log_func,
    provide_context=True,
    dag=dag
    )

import_datagp_to_log

#dummy = DummyOperator(task_id="dummy", dag=dag)

#echo_ds = BashOperator(
    #task_id='echo_ds',
    #bash_command='echo {{ execution_date }}',
    #dag=dag
#)

#def hello_world_func():
    #logging.info("Hello World!")

#hello_world = PythonOperator(
    #task_id='hello_world',
    #python_callable=hello_world_func,
    #dag=dag
#)

#def select_func(**kwargs):
    #execution_date = kwargs['execution_date']
    #if execution_date.weekday == 6:
        #return 'task 2'
    #else:
        #return 'task1'


#select_weekday = BranchPythonOperator(
    #task_id='select_random',
    #python_callable=select_func,
    #dag=dag
#)

#task_1 = DummyOperator(task_id='task_1', dag=dag)
#task_2 = DummyOperator(task_id='task_2', dag=dag)



#dummy >> hello_world >> select_weekday >> [task_1, task_2]