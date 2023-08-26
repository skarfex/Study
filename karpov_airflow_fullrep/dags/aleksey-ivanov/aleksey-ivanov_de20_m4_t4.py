"""
Data Engineer 20
Module 4
Task 4
"""

import logging

import pendulum
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "aleksey-ivanov",
    "poke_interval": 600,
    "start_date": pendulum.datetime(2022, 3, 1, tz="UTC"),
    "end_date": pendulum.datetime(2022, 3, 14, tz="UTC"),
}

with DAG(
    dag_id="aleksey-ivanov_de20_m4_t4",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=["aleksey-ivanov"],
) as dag:

    def get_iso_week_day(str_date):
        isoweekday = pendulum.from_format(str_date, "YYYY-MM-DD").isoweekday()
        return isoweekday

    def is_not_sunday_func(**kwargs):
        ds = kwargs["ds"]
        isoweekday = get_iso_week_day(ds)

        logging.info(f"{ds} => {isoweekday}")
        return isoweekday != 7

    def get_article_heading_func(**kwargs):
        ds = kwargs["ds"]
        isoweekday = get_iso_week_day(ds)

        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum")
        conn = pg_hook.get_conn()
        cursor = conn.cursor("aleksey-ivanov_de20_m4_t4")
        sql = f"SELECT heading FROM articles WHERE id = {isoweekday}"

        cursor.execute(sql)
        query_res = cursor.fetchall()
        logging.info(sql)
        logging.info(query_res)
        return query_res

    is_not_sunday = ShortCircuitOperator(
        task_id="is_not_sunday", python_callable=is_not_sunday_func
    )

    get_article_heading = PythonOperator(
        task_id="get_article_heading", python_callable=get_article_heading_func
    )

    is_not_sunday >> get_article_heading
