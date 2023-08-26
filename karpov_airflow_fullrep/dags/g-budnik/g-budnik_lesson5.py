import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from g_budnik_plugins.g_budnik_ram_species_count_operator import (
    GlebBudnikRamSpeciesCountOperator,
)

DEFAULT_ARGS = {
    "start_date": days_ago(1),
    "end_date": datetime(2023, 6, 20),
    "owner": "gleb_dag",
    "poke_interval": 10,
}

with DAG(
    "gleb_RM",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["gleb_RM"],
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            CREATE TABLE IF NOT EXISTS public.g_budunik_ram_location (
            id INT,
            name text,
            type text,
            dimension text,
            resident_cnt int);
          """,
    )

    clear_table = PostgresOperator(
        task_id="clear_table",
        postgres_conn_id="conn_greenplum_write",
        sql="TRUNCATE TABLE g_budunik_ram_location;",
    )

    data = GlebBudnikRamSpeciesCountOperator(task_id="load_data_from_ip")

    create_pet_table >> clear_table >> data
