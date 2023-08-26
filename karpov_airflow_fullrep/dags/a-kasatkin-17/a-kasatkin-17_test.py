'''
Тестовый даг для
модуль 3 Задание
'''

from airflow import DAG
import airflow
import locale
import logging
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2023, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2023, 3, 14, tz='utc'),
    'owner': 'a-kasatkin-17'
}

dag = DAG(dag_id='a-kasatkin-17_simple_dag',
            schedule_interval='5 4 * * 1/6',
            default_args=DEFAULT_ARGS,
            max_active_runs=1,
            tags=['a-kasatkin-17']
          )
def get_data():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {datetime.now().weekday()+1}')  # исполняем sql
    # query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(one_string)

get_data = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

get_data
