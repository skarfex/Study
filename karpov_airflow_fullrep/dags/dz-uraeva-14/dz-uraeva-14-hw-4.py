import logging
from datetime import date

import pendulum
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator, get_current_context

log = logging.getLogger(__name__)

dag = DAG(
    dag_id="dz-uraeva-14-hw-4",
    schedule_interval="15 3 * * 1-6",
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 15, tz="UTC"),
    max_active_runs=1,
    catchup=True,
    tags=["dz-uraeva-14"]
)


def get_day_num_func():
    context = get_current_context()
    return date.fromisoformat(context['ds']).isoweekday()


def print_heading_func():
    gp_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = gp_hook.get_conn()
    cursor = conn.cursor()
    day_num = get_day_num_func()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_num}')
    query_res = cursor.fetchone()[0]
    log.info(f"Heading of article with id = {day_num} is {query_res}")


print_heading = PythonOperator(
    task_id="print_heading",
    python_callable=print_heading_func,
    dag=dag
)
