'''
lesson 4 pipelines
Schedule
'''

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import ShortCircuitOperator

#from airflow.hooks import PostgresHook
from airflow.hooks.postgres_hook import PostgresHook #  хук для работы с GP
import logging
logging.info(PostgresHook.get_connection('conn_greenplum').password)

DEFAULT_ARGS = {
    'owner': 'ds',
    'email': ['sokolartemy@gmail.com'],
    'email_on_failure': ['sokolartemy@gmail.com'],
    'retries': 15,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    #'sla': timedelta(hours=2),
    'execution_timeout': timedelta(seconds=300),

}

with DAG ("a-sokolov-6-lesson-4",
        schedule_interval='@daily',
default_args=DEFAULT_ARGS,
max_active_runs=3,
description = 'ais-description',
tags=['lesson4', 'ais']

) as dag:
    dummy = DummyOperator(task_id="dummy")
    dummy2 = DummyOperator(task_id="dummy2")

    #ddate = datetime.now().weekday()
    #ddate = datetime.strptime(ds, "%Y-%m-%d").weekday()
    #ds = kwargs['execution_date']
    #ds = dag_run.logical_date
    #ds = {{ execution_date }}

    #ds -  execution_date (if you work with Airflow versions prior to 2.2). Nowadays, we just call it logical_date or ds for short. This is one of the many parameters that you can reference inside your Airflow task.

    def week_day():
        ddate = ds.weekday()
        if 6 != ddate: # note, monday is 0, tuesday is 1... so sunday is 6
            logging.info(f'Its not a sunday {ddate} ')
            return True
        else:
            logging.info(f'Its a sunday {ddate} ')
            return False


    def article():
        ddate = ds.weekday()
        day_of_week = ddate + 1 # +1 т.к. в задаче (понедельник=1, вторник=2, ...)
        logging.info(f'for weekday {day_of_week} :')

        sql_query = f'select heading from articles where id = {day_of_week}'

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        if day_of_week != 7:
            cursor.execute(sql_query)  # исполняем sql
            query_res = cursor.fetchall()  # полный результат
            one_string = cursor.fetchone()[0]  # если вернулось единственное значение
            logging.info(f'for weekday {day_of_week} :')
            logging.info(query_res[0])


    articles = PythonOperator(
        task_id = 'articles',
        python_callable = article,
        dag=dag
    )

    not_sunday = ShortCircuitOperator(
        task_id = 'not_sunday',
        python_callable = week_day,
        dag=dag
    )
    not_sunday.doc_md = 'Hi, these are task docs.'

    dummy >> [articles, not_sunday]
    not_sunday >> dummy2
