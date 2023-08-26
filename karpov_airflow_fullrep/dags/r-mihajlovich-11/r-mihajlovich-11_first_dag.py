"""
Первый даг! Теепрь с заданием часть.2!
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'r-mihajlovich-11',
    'poke_interval': 600,
    'start_date':datetime(2022,3,1),
    'end_date':datetime(2022,3,14)
}

with DAG("r-mihajlovich-11_first_dag",
          schedule_interval='30 16 * * 1-6',
          default_args=DEFAULT_ARGS,
          catchup= True,
          tags=['r-mihajlovich-11']
          ) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def test_func():
        logging.info("It's working!")


    first_task = PythonOperator(
        task_id='first_task',
        python_callable=test_func,
        dag=dag
    )


    def gather_data_func():
        logging.info("Started data gather")
        pg_hook=PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        day_to_grab= datetime.now().isoweekday()
        logging.info(f"Date is {datetime.now()},  day = {day_to_grab}")
        cursor.execute(f"Select heading from articles WHERE id={day_to_grab}")
        result=cursor.fetchone()[0]
        logging.info(result)


    get_date_psgres = PythonOperator(
        task_id='get_date_psgres',
        python_callable=gather_data_func,
        dag=dag
    )

    echo_ds >> [first_task, get_date_psgres]