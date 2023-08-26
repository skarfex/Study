import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    "owner": "a.matjushina",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_week_day():
    context = get_current_context()
    ds = context["ds"]
    ds = pendulum.parse(ds)
    return ds.weekday() + 1


def get_data_from_db():
    week_day = get_week_day()
    pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT heading FROM articles WHERE id = {week_day}")
    try:
        one_string = cursor.fetchone()[0]
        print("Result: ", one_string)
        logging.info(f"Article heading for week_day {week_day} is {one_string}")
    except Exception:
        raise AirflowFailException


with DAG(
    "matjushina_lesson_4",
    default_args=DEFAULT_ARGS,
    description="A simple DAG",
    schedule_interval="0 0 * * 1-6",
    start_date=datetime(2022, 3, 1),
    end_date=datetime(2022, 3, 14),
    catchup=False,
    tags=["a.matjushina"],
) as dag:
    dag.doc_md = """
        DAG for executing data from Postgres DB and logging it out
    """

    t1 = PythonOperator(
        task_id="python_task", depends_on_past=False, python_callable=get_data_from_db,
    )

    t1
