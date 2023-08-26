"""
даг работает с понедельника по субботу, но не по воскресеньям,
загружает из GreenPlum из таблицы articles значение поля heading из строки с id, равным дню недели ds
(понедельник=1, вторник=2, ...)
"""
from airflow import DAG
from airflow.utils.dates import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

with DAG("a_tarhova_lesson_4_practice",
         start_date=datetime(2022, 3, 1),
         end_date=datetime(2022, 3, 14),
         owner='a-tarhova',
         poke_interval=600,
         schedule_interval='0 5 * * 1,2,3,4,5,6',
         tags=['a-tarhova']
         ) as dag:

    def is_week_without_Sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').isoweekday()
        return exec_day in [1, 2, 3, 4, 5, 6]

    dummy = DummyOperator(task_id='dummy')
    w_d = is_week_without_Sunday_func('{{ ds }}')

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}, w_d',
        dag=dag
    )

    def load_greenplum_articles_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        w_d = is_week_without_Sunday_func('{{ ds }}')
        cursor.execute('''SELECT heading FROM articles WHERE id = ?''', w_d)
        #query_res = cursor.fetchall()
        #one_string = cursor.fetchone()[0]


    load_greenplum_articles = PythonOperator(
        task_id='load_greenplum_articles_func',
        python_callable=load_greenplum_articles_func,
        dag=dag
    )

    dummy >> echo_ds >> load_greenplum_articles
