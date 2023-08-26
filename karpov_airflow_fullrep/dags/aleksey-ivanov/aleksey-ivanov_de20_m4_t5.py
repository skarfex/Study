# -*- coding: utf-8 -*-

"""
Data Engineer 20
Module 4
Task 5
"""

from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from aleksey_ivanov_plugins.de20_m4_t5_ram_operator import DE20M4T5RamOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "aleksey-ivanov",
    "poke_interval": 600,
    "start_date": days_ago(1),
}

with DAG(
    dag_id="aleksey-ivanov_de20_m4_t5",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=["aleksey-ivanov"],
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        sql="de20_m4_t5_create_table.sql",
    )

    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="conn_greenplum_write",
        sql="de20_m4_t5_truncate_table.sql",
    )

    insert_top_locations = DE20M4T5RamOperator(
        task_id="insert_top_locations",
        number_of_locations=3,
    )

    create_table >> truncate_table >> insert_top_locations
