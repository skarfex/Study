from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
import logging
import random
from airflow.operators.python import get_current_context
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'v-mashkina-11',
    'poke_interval': 600
}

with DAG("v-mashkina-11_lesson_4",
          #schedule_interval='@daily',
          schedule_interval='30 9 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-mashkina-11']
          ) as dag:

    start = DummyOperator(task_id='start')

    def check_day_func():
        context = get_current_context()
        ds = context["ds"]
        if datetime.strptime(str(ds), '%Y-%m-%d').weekday() == 6:
            return 'sunday'
        else:
            return 'get_heading'

    check_day = BranchPythonOperator(
        task_id='check_day',
        python_callable=check_day_func,
        provide_context=True,
    )


    def get_heading_func():
        context = get_current_context()
        ds = context["ds"]
        date = str(ds)
        weekday = datetime.strptime(date, '%Y-%m-%d').weekday()+1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(weekday))
        query_res = cursor.fetchall()
        #one_string = cursor.fetchone()[0]
        logging.info(f'Value: {query_res}, Weekday: {weekday}, Date: {datetime.strptime(date, "%Y-%m-%d")}')
        #logging.info(f'Value: {one_string}, Weekday: {weekday}, Date: {datetime.strptime(date, "%Y-%m-%d")}')

    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_func,
        provide_context=True,
        dag=dag
    )

    sunday = DummyOperator(task_id='sunday')

    end = DummyOperator(task_id='end', trigger_rule='one_success')

    start >> check_day >> [get_heading, sunday] >> end