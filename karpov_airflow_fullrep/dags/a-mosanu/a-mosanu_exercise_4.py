"""
Exercise 4.1
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.edgemodifier import Label
from datetime import datetime
import pendulum

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='UTC'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='UTC'),
    'owner': 'a-mosanu',
    'poke_interval': 600
}

with DAG("Chapter_4",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mosanu']
) as dag:

    dummy = DummyOperator(task_id="DummyOperator")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

 #Add short circuit operator
    def except_sunday_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').isoweekday()
        return exec_day < 7

    except_sunday = ShortCircuitOperator(
        task_id='except_sundays',
        python_callable=except_sunday_func,
        op_kwargs={'execution_dt': '{{ ds }}'} )

    def hello_world_func():
        logging.info("My first ever DAG!")

    hello_world = PythonOperator(
        task_id='first',
        python_callable=hello_world_func
    )

    def article_head(**kwargs):
        current_weekday_number = datetime.strptime(kwargs['ds'], '%Y-%m-%d').isoweekday()
        logging.info('--------------')
        kwargs['ti'].xcom_push(value=current_weekday_number, key='weekday')
        logging.info("Current date %s, \n weekday number: %d", kwargs['ds'], current_weekday_number)

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {current_weekday_number}')
        result = cursor.fetchone()[0]
        logging.info("Heading: \n%s", result)
        logging.info('--------------')
        kwargs['ti'].xcom_push(value=result, key='DB_result')


    current_day_heading_logging = PythonOperator(
        task_id='headding_logging',
        python_callable=article_head,
        provide_context=True
    )

    except_sunday >> dummy >> Label("Echo datetime") >> echo_ds >> current_day_heading_logging
    except_sunday >> dummy >> Label("Python script") >> hello_world >> current_day_heading_logging
