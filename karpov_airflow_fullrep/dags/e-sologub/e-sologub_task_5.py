from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from e_sologub_plugins.e_sologub_ram_location import (
    ESologubRamLocationOperator,
)

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-sologub',
    'poke_interval': 600,
    "depends_on_past": False
}

with DAG(
    "e-sologub_task_5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    description="Top-3 locations of Rick and Morty",
    catchup=False,
    tags=["e-sologub"],
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"""CREATE TABLE IF NOT EXISTS e_sologub_ram_location (
            id serial NOT NULL,
            "name" varchar NOT NULL,
            "type" varchar NOT NULL,
            dimension varchar NOT NULL,
            resident_cnt int NOT NULL
        )""",
    )

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="conn_greenplum_write",
        trigger_rule="all_success",
        sql=f"TRUNCATE e_sologub_ram_location",
    )

    top3_locations = ESologubRamLocationOperator(task_id="top3_locations")

    create_table >> truncate_table >> top3_locations
