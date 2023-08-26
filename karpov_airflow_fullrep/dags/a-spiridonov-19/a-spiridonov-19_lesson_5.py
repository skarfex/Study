"""
Lesson 5
"""
import pendulum

from a_spiridonov_19_plugins.a_spiridonov_19_lesson_5_plugin import RMGetPopularLocationOperator
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 4, 14, 3, tz="UTC"),
    "owner": "a-spiridonov-19",
    "email": ["spiridonovandrei@hotmail.com"],
    "email_on_failure": True,
    "retries": 2,
}
TABLE_NAME = '"a-spiridonov-19_ram_location"'

with DAG(
    dag_id="a-spiridonov-19_lesson_5",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
) as dag:

    get_data = RMGetPopularLocationOperator(
        task_id="get_data",
        top_n=3,
        table=TABLE_NAME
    )

    truncate_target_table = PostgresOperator(
        task_id="truncate_target_table",
        postgres_conn_id='conn_greenplum_write',
        sql=f'TRUNCATE TABLE {TABLE_NAME}',
    )

    load_data = PostgresOperator(
        task_id="load_data",
        postgres_conn_id='conn_greenplum_write',
        sql="{{ ti.xcom_pull(task_ids='get_data') }}"
    )

    get_data >> truncate_target_table >> load_data
