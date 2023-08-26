"""
Создание DAG для получения данных из ЦБ
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import csv

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 2, 28),
    'end_date': datetime(2022, 3, 14),
    'owner': 'Dariya',
    'poke_interval': 600
}

with DAG("d-vorobeva_heading",
          schedule_interval="0 12 * * 1-6",
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d-vorobeva']
          ) as dag:

    dummy_start = DummyOperator(task_id='start')
    dummy_end = DummyOperator(task_id='end')

    def select_from_db(data):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        day_of_week = str(datetime.strptime(str(data), '%Y-%m-%d').weekday() + 1)
        print('day_of_week=' + day_of_week)
        cur.execute("SELECT heading FROM articles WHERE id={day_of_week};".format(day_of_week=day_of_week))
        one_string = cur.fetchone()[0] + '  ds :' + str(day_of_week)
        return one_string

    select_heading = PythonOperator(
        task_id='select_heading',
        python_callable=select_from_db,
        op_args = ['{{ ds }}'],
        dag=dag
    )

    dummy_start >> select_heading >> dummy_end

