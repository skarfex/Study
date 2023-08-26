"""
Lesson 4

Changed start date and end date #1
Working schedule added #2
Greenplum connection added #3
Cursor selection based on week day number #4
"""
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
import csv
import logging
import pendulum


DEFAULT_ARGS = {

    'owner': 'o-anchikov-17',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(seconds=5),
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"), # 1
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'poke_interval': 300,
    'trigger_rule': 'all_success'
}

with DAG("o-anchikov-17_lesson4",
    schedule_interval='0 1 * * 1-6', # 2
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-anchikov-17_lesson4'],
    catchup=True
) as dag:

    # functions

    def hello_world_func():
        time = datetime.today()
        print ('hello_world! timestamp logged!')
        logging.info("current_date: "+str(time))

    def get_heading_func(ds, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # 3
        conn = pg_hook.get_conn()


        cursor = conn.cursor("named_cursor_name")
        week_day = datetime.strptime(ds, '%Y-%m-%d').isoweekday()
        logging.info(f"Select article heading for {week_day} day")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day}')  # 4
        query_res = cursor.fetchone()[0]

        conn.close()
        logging.info(f"Result: '{query_res}'")
        kwargs['ti'].xcom_push(value=query_res, key='heading')


    # operators

    dummy_start = DummyOperator(task_id="dag_start")
    dummy_end = DummyOperator(task_id="dag_end")

    bash_counter = BashOperator(
        task_id='bash_counter',
        bash_command='sleep 0.5 && echo “I’ve been sleeping for 0.5 seconds, I want more” && sleep 0.5 && echo “I’m done sleeping, thanks!”',
        dag=dag
    )

    python_op = PythonOperator(
        task_id='python_task',

        python_callable=hello_world_func,
        dag=dag
    )


    get_heading = PythonOperator(
        task_id='get_heading_from_db',
        op_kwargs={'ds': "{{ ds }}"},  # The DAG run’s logical date as YYYY-MM-DD
        python_callable=get_heading_func,
        provide_context=True
    )

    # scheme

    dummy_start >> bash_counter >> python_op >>get_heading>> dummy_end
