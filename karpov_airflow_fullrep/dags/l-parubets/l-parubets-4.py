''' Getting data from GP'''
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, date


DEFAULT_ARGS = {
    'owner': 'l-parubets',
    # 'start_date': days_ago(0),
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'catchup': True,
    'poke_interval': 600
}


def read_from_gp():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("cursor")  # и именованный (необязательно) курсор
    dt = date.today().isoweekday()
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(dt))  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    for row in query_res:
        logging.info('query result is : {}'.format(row[0]))


with DAG(
        'l-parubets-4',
        default_args=DEFAULT_ARGS,
        schedule_interval='1 0 * * 1-6',
        catchup=False,
        description='selecting data from GP',
        tags=['l-parubets']
) as dag:
    t1 = PythonOperator(
        task_id='show_query_result',
        python_callable=read_from_gp
    )
    t2 = BashOperator(
        task_id='print_datetime',
        bash_command='date',
    )

    t1 >> t2

