"""
Lesson 4
Simple dag 2
"""
import logging

from airflow import DAG

from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': datetime (2022, 3, 1), # указываем дату старта ДАГа
    'end_date': datetime (2022, 3, 14), # указываем дату завершения работы ДАГа
    'owner': 'a-kapaev-20'
}

with DAG('a-kapaev-20-module_2-lesson_4',
    schedule_interval='0 9 * * MON-SAT', # указываем график работы ДАГа с Пн по Сб в 6 утра (UTC)
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-kapaev-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def data_GP(execute):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        date_format = '%Y-%m-%d'
        date_execute = datetime.strptime(execute, date_format).date()
        date_of_week_execute = date_execute.isoweekday()
        query_sql = (f""" SELECT heading from public.articles where id = {date_of_week_execute}""")
        cursor.execute(query_sql)
        result = cursor.fetchall()
        logging.info(result)

        cursor.close()
        conn.close


    date_from_gp = PythonOperator(
        task_id='date_from_gp',
        python_callable=data_GP,
        op_kwargs={'execute': '{{ ds }}'},
        dag=dag
    )

    dummy >> date_from_gp