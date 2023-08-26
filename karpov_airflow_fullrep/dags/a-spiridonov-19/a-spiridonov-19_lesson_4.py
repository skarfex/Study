"""
Lesson 4
"""
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


def get_data(execution_dt):
    today_index = pendulum.from_format(execution_dt, 'YYYY-MM-DD').add(days=1).day_of_week
    query = f"SELECT heading FROM articles WHERE id = '{today_index}'"

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    query_res = cursor.fetchall()
    print(query_res)

    return query_res

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 2, 28, tz="UTC"),
    "end_date": pendulum.datetime(2022, 3, 13, tz="UTC"),
    "owner": "a-spiridonov-19",
    "email": ["spiridonovandrei@hotmail.com"],
    "email_on_failure": True,
    "retries": 2,
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="a-spiridonov-19_lesson_4",
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    # catchup=False
) as dag:

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )
