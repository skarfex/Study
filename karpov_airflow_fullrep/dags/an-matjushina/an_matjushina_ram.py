from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from an_matjushina_plugins.operators.ram_top_locations_operator import (
    MatjushinaRaMOperator,
)

TABLE_NAME = "an_matjushina_ram_location"


DEFAULT_ARGS = {
    "owner": "a.matjushina",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "matjushina_lesson_6",
    default_args=DEFAULT_ARGS,
    description="Top-3 locations of Rick and Morty DAG",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=["a.matjushina"],
) as dag:
    dag.doc_md = """
        DAG for executing data from Postgres DB and logging it out
    """

    get_data = MatjushinaRaMOperator(task_id="get_ram_top3_location",)

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id  INTEGER,
            name VARCHAR,
            type VARCHAR,
            dimension VARCHAR,
            resident_cnt INTEGER
        )""",
    )

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"TRUNCATE {TABLE_NAME}",
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"INSERT INTO {TABLE_NAME} (id, name, type, dimension, resident_cnt) "
        "VALUES {{ ti.xcom_pull(task_ids='get_ram_top3_location') }}",
    )

    get_data >> create_table >> truncate_table >> insert_data
