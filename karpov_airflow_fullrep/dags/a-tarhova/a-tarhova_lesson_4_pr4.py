"""
даг работает с понедельника по субботу, но не по воскресеньям,
загружает из GreenPlum из таблицы articles значение поля heading из строки с id, равным дню недели ds
(понедельник=1, вторник=2, ...)
"""
from airflow import DAG
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-tarhova',
    'poke_interval': 600
}

with DAG("a_tarhova_lesson_4_pr4",
         schedule_interval='0 5 * * 1,2,3,4,5,6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-tarhova']
         ) as dag:

    def get_num_day_func(**kwargs):
        print(f"ds = {kwargs['ds']}")
        num_day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').isoweekday()
        print(f"num_day = {str(num_day)}")
        return num_day


    def select_str_func(**kwargs):
        num_day = kwargs['templates_dict']['num_day']
        conn = PostgresHook(postgres_conn_id='conn_greenplum').get_conn()
        cursor = conn.cursor('cursor')
        cursor.execute(f'SELECT heading FROM articles WHERE id ={num_day} limit 1')
        result_str = cursor.fetchone()[0]
        kwargs['ti'].xcom_push(value=result_str, key='result_str')


    def print_str_func(**kwargs):
        print(kwargs['ti'].xcom_pull(task_ids='select_str', key='result_str'))

    dummy = DummyOperator(task_id="dummy")

    get_num_day = PythonOperator(
        task_id='get_num_day',
        python_callable=get_num_day_func,
        provide_context=True
    )

    select_str = PythonOperator(
        task_id='select_str',
        python_callable=select_str_func,
        templates_dict={'num_day': '{{ ti.xcom_pull(task_ids="get_num_day", key="return_value") }}'},
        provide_context=True
    )
    print_str = PythonOperator(
        task_id='print_str',
        python_callable=print_str_func,
        provide_context=True
    )

    dummy >> get_num_day >> select_str >> print_str
