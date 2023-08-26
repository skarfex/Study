from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.sensors import time_delta_sensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 60,
    'owner': 'a-gurov-16'
}

pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук


def extract_data_from_gp(**kwargs):
    execution_date = kwargs['execution_date']
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()  # и именованный (необязательно) курсор
    logging.info('EXECUTION_DATE: {}'.format(execution_date.weekday() + 1))
    cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(execution_date.weekday() + 1))  # исполняем sql
    # query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info('DATABASE ANSWER: {}'.format(one_string))


dag = DAG("a-gurov-16_lesson4_dag",
          schedule_interval='0 18 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          catchup=True
          )

t1 = PythonOperator(
        task_id='gp_extractor',
        python_callable=extract_data_from_gp,
        dag=dag
    )