"""
Даг из задания к уроку Сложные пайплайны ч.2


"""

from airflow import DAG
import logging
import pendulum
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'ang-semenova',
    'start_date': pendulum.datetime(2022, 3, 1, tz = 'UTC'),
    'end_date': pendulum.datetime(2022, 3, 14, tz = 'UTC')
}

    
with DAG("ang-semenova-hw2",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ang-semenova']
        ) as dag:
    
    start = DummyOperator(
        task_id = 'start'
    )
        
    def is_it_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day not in [6]

    is_it_sunday = ShortCircuitOperator(
        task_id = 'is_it_sunday',
        python_callable = is_it_sunday_func,
        op_kwargs = {'execution_dt': '{{ds}}' }
    )


    def postgreshook_func(ds):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name") 
        cursor.execute('SELECT heading FROM articles WHERE id = {weekday}'.format(weekday=datetime.strptime(ds, '%Y-%m-%d').isoweekday())) 
        query_res = cursor.fetchall()  
        logging.info(query_res[0])
        

    article_head = PythonOperator(
        task_id='article_head',
        python_callable= postgreshook_func,
        op_kwargs={'execution_dt': '{{ds}}'}

    )


    start >> is_it_sunday >> article_head

