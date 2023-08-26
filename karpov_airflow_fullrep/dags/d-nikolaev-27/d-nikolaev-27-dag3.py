from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from d_nikolaev_27_plugins.d_nikolaev_27_ram_location_operator import Rick_and_Morty_Nikolaev_Operator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-nikolaev-27',
    'poke_interval': 600
}

with DAG('d-nikolaev-27-rick_and_morty',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-nikolaev-27-rick_and_morty']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    show_top_locations = Rick_and_Morty_Nikolaev_Operator (
            task_id = 'show_top_locations',
            dag = dag)

    sql_create_table = """CREATE TABLE if not exists  d_nikolaev_27_ram_location
            ( id text, name text,  type text,  dimension text, resident_cnt text) DISTRIBUTED BY (id);
            TRUNCATE TABLE d_nikolaev_27_ram_location;"""

    sql_write = """
        INSERT INTO d_nikolaev_27_ram_location 
        VALUES {{ ti.xcom_pull(task_ids='show_top_locations', key='written_dataframe') }};
    """

    create_GP_table = PostgresOperator(
        task_id="prepare_gp_table",
        postgres_conn_id="conn_greenplum_write",
        sql=sql_create_table,
        dag = dag
    )

    write_to_greenplum_task = PostgresOperator(
        task_id="load_to_gp",
        postgres_conn_id="conn_greenplum_write",
        sql= sql_write,
        dag = dag
    )

    dummy_end = DummyOperator(task_id="dummy")

    dummy >> show_top_locations >> create_GP_table >> write_to_greenplum_task >> dummy_end