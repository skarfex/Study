"""
Simple dag 2 (Airflow, les 4)
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook # c помощью этого hook будем входить в наш Greenplan

import logging
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime (2022, 3, 1), # указываем дату старта ДАГа
    'end_date': datetime (2022, 3, 14), # указываем дату завершения работы ДАГа
    'owner': 'golovenkin.egor',
    'provide_context' : True
}

with DAG('e-golovenkin-20-les4',
    schedule_interval='0 9 * * MON-SAT', # указываем график работы ДАГа с Пн по Сб в 6 утра (UTC)
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=True,
    tags=['gol']
) as dag:

    def get_head (**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("gp_conn")  # и именованный (необязательно) курсор

        week_day_num = (datetime.isoweekday ( datetime.strptime(kwargs['ds'],"%Y-%m-%d") ) )

        week_day_descr = ''
        if week_day_num == 1:
            week_day_descr = 'Monday'
        elif week_day_num == 2:
            week_day_descr = 'Tuesday'
        elif week_day_num == 3:
            week_day_descr = 'Wednesday'
        elif week_day_num == 4:
            week_day_descr = 'Thursday'
        elif week_day_num == 5:
            week_day_descr = 'Friday'
        elif week_day_num == 6:
            week_day_descr = 'Saturday'

        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day_num}')  # исполняем sql
        query_res = cursor.fetchall()[0]
        logging.info(f"execution_date - {kwargs['ds']}\nWeekday - {week_day_num} / {week_day_descr}\nHeading = {query_res}")

    get_data_from_gp = PythonOperator(
        task_id='get_data_from_gp',
        python_callable=get_head
    )

    get_data_from_gp
