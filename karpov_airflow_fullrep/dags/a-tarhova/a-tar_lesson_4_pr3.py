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

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-tar',
    'poke_interval': 600
}
with DAG("a_tar_lesson_4_pr3",
         default_args=DEFAULT_ARGS,
         schedule_interval='0 5 * * 1,2,3,4,5,6',
         tags=['a-tar']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    w_d = datetime.strptime('{{ ds }}', '%Y-%m-%d').isoweekday()

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}, w_d',
        dag=dag
    )

    def load_greenplum_articles_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        w_d = datetime.strptime('{{ ds }}', '%Y-%m-%d').isoweekday()
        cursor.execute('''SELECT heading FROM articles WHERE id = ?''', w_d)
        #query_res = cursor.fetchall()
        #one_string = cursor.fetchone()[0]


    load_greenplum_articles = PythonOperator(
        task_id='load_greenplum_articles_func',
        python_callable=load_greenplum_articles_func,
        dag=dag
    )

    dummy >> echo_ds >> load_greenplum_articles
