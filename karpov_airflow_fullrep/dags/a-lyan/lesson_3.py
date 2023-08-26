"""Init simple dag."""
import datetime as dt
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {"owner": "a-lyan", "poke_interval": 600}

dag = DAG(
    "a-lyan-lesson-3",
    start_date=dt.datetime(2022, 3, 1),
    end_date=dt.datetime(2022, 3, 14),
    schedule_interval="* * * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["lyan", "lesson-3"],
)


def print_data(execution_dt):
    psql_conn = PostgresHook(postgres_conn_id="conn_greenplum")

    conn = psql_conn.get_conn()  # берём из него соединение
    cursor = conn.cursor("lesson-3")  # и именованный (необязательно) курсор

    weekday = dt.datetime.strptime(execution_dt, "%Y-%m-%d")

    weekday = weekday.weekday() + 1

    cursor.execute(
        f"SELECT id, heading FROM articles WHERE id = {weekday}"
    )
    query_res = cursor.fetchone()[0]

    logging.info(f"{query_res}")
    print(f"{query_res}")


dummy = DummyOperator(task_id="dummy", dag=dag)


print_date = PythonOperator(
    task_id="print_date",
    python_callable=print_data,
    provide_context=True,
    op_kwargs={"execution_dt": "{{ ds }}"},
)


dummy >> print_date
