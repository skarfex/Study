"""
забираем данные из cbr и складываем в greenplum
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime


from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': days_ago(12),
    'owner': 'v-burov-12',
}

with DAG("v-burov-12_dag",
          schedule_interval='0 1 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-burov-12']
          ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def get_db_data():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {datetime.today().isoweekday()}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        print(f"print: {query_res}")
        logging.info(f"logging: {query_res}")

    get_db_data_task = PythonOperator(
        task_id='show_article',
        python_callable=get_db_data
    )


dummy >> get_db_data_task