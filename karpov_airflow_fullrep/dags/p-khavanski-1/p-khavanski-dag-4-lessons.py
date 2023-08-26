"""

DAG для загрузки данных из GreenPlum по расписанию пн-сб

"""


import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600,
    'owner': 'p-khavanski',
    'catchup': True
}
# Делаем запрос в GreenPlum
def get_heading_func(day_of_week, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
    query_res = cursor.fetchall()
    logging.info('FETCH ALL = ', query_res)


with DAG("p-khavanski",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-khavanski']) as dag:

    get_articles_head = PythonOperator(
        task_id='get_articles_heading',
        python_callable=get_heading_func,
        op_args=['{{ logical_date.weekday() + 1 }}']
    )




    get_articles_head