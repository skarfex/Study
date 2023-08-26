"""
Даг забирает из таблицы articles значение поля heading для каждого дня недели,
кроме воскресенья
"""
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'owner': 'n-kalimanov-19',
    'poke_interval': 6
}
day_of_week = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']


with DAG("n-kalimanov_data_greenplum",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n-kalimanov-19'],
) as dag:

    def select_article(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        logging.info(f'Сегодня {day_of_week[exec_day]}')

        if exec_day < 6:
            pg_hook = PostgresHook('conn_greenplum')
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day+1}')
            query_res = cursor.fetchone()[0] # возвращаем единственное значение
            conn.close()
            logging.info(f'Результат: {query_res}')
        else:
            logging.info('Ничего не делаем')

    article = PythonOperator(
        task_id='weekend_only',
        python_callable=select_article,
        op_kwargs={'execution_dt': '{{ ds }}'},
    )

    end_task = DummyOperator(task_id="end_task")

    article >> end_task