"""
Сложный dag.

"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 's-dobrynin',
    'poke_interval': 600
}

with DAG (
    "s-dobrynin-dag2",
    schedule_interval='0 1 * * 1-6', #в час ночи с пон по субботу, https://crontab.guru/#0_1_*_*_1-6
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-dobrynin']
    ) as dag:


    def connect_func(**kwargs):
        logging.info ('--------------')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()  # берём из него соединение
        day_of_week = datetime.now().isoweekday() #номер дня недели, 1 понедельник
        cursor = conn.cursor("s_dobrynin_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = { day_of_week }')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('--------------')
        return query_res

    dummy = DummyOperator(task_id="dummy")

    connect_operator = PythonOperator (task_id = 'connect_operator',
                                       python_callable = connect_func,
                                       dag=dag)
    def print_args_func (**kwargs):
        logging.info ('--------------')
        logging.info('template_dict, kwarg1: ' + kwargs['templates_dict']['kwarg1'])
       # logging.info('templates_dict, kwarg2: ' + kwargs['templates_dict']['kwarg2'])
        logging.info('--------------')


    print_args = PythonOperator(
        task_id='print_args',
        python_callable=print_args_func,
        templates_dict = {'kwarg1': '{{ti.xcom_pull(task_ids = "connect_operator")}}'} ,#Возвращаем результат xcomm, читает. Второй способ Jinja
        provide_context = True
    )

    dummy >> connect_operator >> print_args
