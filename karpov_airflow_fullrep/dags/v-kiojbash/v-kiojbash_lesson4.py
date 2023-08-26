'''
 Нужно доработать даг, который вы создали на прошлом занятии.
Он должен:
- Работать с понедельника по субботу, но не по воскресеньям
- Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

 Забирать из таблицы articles значение поля heading из строки с id,
равным дню недели ds (понедельник=1, вторник=2, ...)

 Выводить результат работы в любом виде: в логах либо в XCom'е
 Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
'''

import logging
from airflow import DAG
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-kiojbash',
    'poke_interval': 600
}

with DAG('v-kiojbash-lesson4',
         default_args=DEFAULT_ARGS,
         schedule_interval='0 0 * * 1-6',  # Кроме воскресенья
         max_active_runs=1,
         tags=['v-kiojbash']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")


    def extract_from_gp_func(**kwargs):
        day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')
        res = cursor.fetchone()[0]
        logging.info(f'Result: {res}')

    extract_from_gp = PythonOperator(
        task_id='extract_from_gp',
        python_callable=extract_from_gp_func
    )

    dummy >> extract_from_gp
