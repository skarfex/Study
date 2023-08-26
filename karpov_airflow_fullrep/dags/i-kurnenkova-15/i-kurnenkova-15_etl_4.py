"""
Дог должен:
Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
Используйте соединение 'conn_greenplum'

Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
import logging

from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'i-kurnenkova-15',
    'poke_interval': 600
}

with DAG("i-kurnenkova-15_etl_4",
    schedule_interval = '* * * * 1-6',
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    catchup=True,
    tags = ['i-kurnenkova-15']
) as dag:

    def get_gp_data_func(day):
        logging.info('<-----------------------------------------------')
        logging.info('Get GP data start')

        logging.info('Day: '.join(day))
        num_day = datetime.strptime(day, '%y-%m-%d').isoweekday()
        logging.info('Num_day: '.join(num_day))

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()

        cursor = conn.cursor('get_gp_data')
        sql = f'SELECT * FROM articles WHERE id={num_day}'
        cursor.execute(sql)
        query_res = cursor.fetchall()
        logging.info('Result: ')
        logging.info(query_res)
        logging.info('----------------------------------------------->')

    get_gp_data = PythonOperator(
        task_id='get_gp_data',
        python_callable=get_gp_data_func,
        op_args=['{{ ds }}']
    )

    get_gp_data
