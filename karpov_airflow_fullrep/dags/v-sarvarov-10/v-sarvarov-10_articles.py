"""
Load Article Headings Based on Weekday Number
"""

from airflow import DAG
import logging
from airflow.sensors.weekday import DayOfWeekSensor
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.dummy import DummyOperator


DEFAULT_ARGS = {
    'owner': 'v-sarvarov-10',
    'poke_interval': 600,
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15)
}

dag = DAG("vs_load_heading",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-sarvarov-10']
    )

weekend_check = DayOfWeekSensor(
    task_id='weekday_check_task',
    week_day={'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'},
    use_task_execution_day=True,
    mode='reschedule',
    dag=dag)

def load_heading_func(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    day_of_week = kwargs['execution_date'].weekday() + 1
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
    query_result = cursor.fetchall()
    kwargs['ti'].xcom_push(value=query_result, key=f'heading_{day_of_week}')
    logging.info('--------------')
    logging.info(f'Day of week: {day_of_week}')
    logging.info(f'Heading: {query_result}')
    logging.info('--------------')


load_heading = PythonOperator(
    task_id='load_heading',
    python_callable=load_heading_func,
    provide_context=True,
    dag=dag
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> weekend_check >> load_heading >> end