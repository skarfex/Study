"""
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 4),# DAG Run start date
    'end_date': datetime(2022, 3, 14, 4),
    'owner': 'p-balandin-16',
    'poke_interval': 600
}

with DAG("p-balandin-16-les-4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p-balandin-16']
) as dag:

    def get_gp_article(week_day, **kwargs):
        pg_hook = PostgresHook('conn_greenplum') # GP connect
        id = datetime.strptime(week_day, '%Y-%m-%d').isoweekday()
        logging.info(f"День недели: {id}")
        query_res = 'Ничего'
        if id < 7:
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f'SELECT heading FROM articles WHERE id = {id}')
            query_res = cursor.fetchone()[0]
            conn.close()
        else:
            logging.info('Sunday')

        logging.info(f"Результат: '{query_res}'")
        kwargs['ti'].xcom_push(value=query_res, key='heading')

    select_text = PythonOperator(
        task_id='select_text',
        op_kwargs={'week_day': "{{ ds }}"},
        python_callable=get_gp_article,
        provide_context=True,
        dag = dag
    )

    end_task = DummyOperator(task_id='end_task')

    select_text >> end_task