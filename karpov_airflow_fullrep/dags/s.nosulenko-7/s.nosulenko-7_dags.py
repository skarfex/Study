# check it works
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import random
import time
import logging

DEFAULT_ARGS = {
    'owner': 'snosulenko',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 3,
    'trigger_rule':  'all_success',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),

}

def check_dow_func(**kwargs):
    this_date = kwargs['ds']
    dow = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
    kwargs['ti'].xcom_push(key='dow', value=dow)
    
    

    
def decide_to_run_func(**kwargs):
    dow = kwargs['ti'].xcom_pull(key='dow', task_ids='check_dow')
    if dow == 6:
        return 'bad_day'
    else:
        return 'get_heading'
    
    

def get_heading_func(**kwargs):
    dow = kwargs['ti'].xcom_pull(key='dow', task_ids='check_dow')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    sql = f'SELECT heading FROM articles WHERE id = {dow}'
    cursor.execute(sql)  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(sql)
    logging.info(query_res)
    

    

with DAG(
    dag_id='sn_complex_pipe_p2',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov', 'snosulenko']
    ) as dag:
    
    start = DummyOperator(task_id='start')
    
    check_dow = PythonOperator(task_id='check_dow', python_callable=check_dow_func, provide_context=True)
    decide_to_run = BranchPythonOperator(task_id='decide_to_run_func', python_callable=decide_to_run_func, provide_context=True)
    
    get_heading = PythonOperator(task_id='get_heading', python_callable=get_heading_func, provide_context=True)
    bad_day = DummyOperator(task_id='bad_day')

    # start >> random_op >> [task_1_op, task_2_op, task_3_op]
    start >> check_dow >> decide_to_run >> [get_heading, bad_day]
