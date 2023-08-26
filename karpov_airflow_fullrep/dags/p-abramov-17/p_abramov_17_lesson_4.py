"""
    Задание 4
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'p-abramov-17',
    'start_date': '2022-03-01',
    'end_date': '2022-03-14'
}

with DAG('p_abramov_17_lesson_4',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p_abramov_17']
         ) as dag:
    def py_day_of_week(**params):
        return datetime.strptime(params['date'], '%Y-%m-%d').date().isoweekday()


    def py_check_flag_start(**context):
        day_of_week = py_day_of_week(date=context['ds'])
        print('-----------------')
        print(f'День недели {day_of_week}')  # логируем
        print('-----------------')
        return day_of_week != 7


    def py_get_info_from_GP(**context):
        print('-----------------')
        print('начинаем выгрузку')  # логируем
        print('-----------------')

        week_day = py_day_of_week(date=context['ds'])

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')
        query_res = cursor.fetchall()
        logging.info(query_res)


    check_flag_start = ShortCircuitOperator(
        task_id='check_flag_start',
        python_callable=py_check_flag_start
    )

    get_info_from_GP = PythonOperator(
        task_id='get_info_from_GP',
        python_callable=py_get_info_from_GP
    )

    check_flag_start >> get_info_from_GP
