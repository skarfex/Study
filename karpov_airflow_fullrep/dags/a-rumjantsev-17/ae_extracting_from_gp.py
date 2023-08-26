"""
Простой даг для пробы
после просмотра первой практики
"""

from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'a-rumjantsev-17',
    'shedule_interval': '0 16 * * 1-6',
    'poke_interval': 600
}

with DAG("ae_extract_from_gp_dag",
        default_args=DEFAULT_ARGS,
        catchup=True,
        tags=['a-rumjantsev-17']
        ) as dag:

    start = DummyOperator(task_id='start')

    def what_is_weekday(ds):
        today_weekday = datetime.strptime(ds, '%Y-%m-%d').isoweekday()
        return today_weekday

    today_weekday = PythonOperator(
        task_id='today_weekday',
        python_callable=what_is_weekday,
        provide_context=True
    )

    def get_data_from_gp(**kwargs):
        day_of_week = kwargs['ti'].xcom_pull(task_ids='today_weekday')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') #инициализируем хук
        conn = pg_hook.get_conn() #берем из него подключение
        cursor = conn.cursor("named_cursor_name") #создаем курсор (именнованный (необязательно))
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week};')
        query_res = cursor.fetchall()
        logging.info(query_res)
        #one_string = cursor.fetchone()[0]
        kwargs['ti'].xcom_push(value=query_res, key='article')
        return query_res

    get_data_articles = PythonOperator(
        task_id='get_data_articles',
        python_callable=get_data_from_gp,
        provide_context=True,
        do_xcom_push=True
    )

    def print_data_to_log(**kwargs):
        this_day = datetime.now()
        logging.info(f'Now is {this_day}')
        data = kwargs['ti'].xcom_pull(task_ids='get_data_articles', key='article')
        logging.info(data)

    printing_data = PythonOperator(
        task_id='printing_data',
        python_callable=print_data_to_log
    )

    start >>today_weekday>>get_data_articles>>printing_data


