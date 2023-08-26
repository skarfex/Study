"""
Module 2 Step 5
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from k_valiev_plugins.k_valiev_ram_top_locations_operator import ValievRamTopLocationsOperator


TABLE_NAME = 'k_valiev_ram_top_locations'
CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'k-valiev',
    'poke_interval': 600
}

with DAG(
    dag_id='k-valiev_ram_top_locations',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=True,
    tags=['k-valiev']
) as dag:

    create_table=PostgresOperator(
        task_id='create_table',
        postgres_conn_id=CONN_ID,
        autocommit=True,
        sql=f"""CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
                    id              bigint PRIMARY KEY NOT NULL
                    ,name           varchar(255) NOT NULL
                    ,type           varchar(255) NOT NULL
                    ,dimension      varchar(255) NOT NULL
                    ,residents_cnt  bigint NOT NULL
                    )
                """
    )


    clear_table=PostgresOperator(
        task_id='clear_table',
        postgres_conn_id=CONN_ID,
        autocommit=True,
        sql=f""" TRUNCATE TABLE public.{TABLE_NAME}"""
    )

    insert_top_locations=ValievRamTopLocationsOperator(
        task_id='insert_top_locations',
        conn_id=CONN_ID,
        table_name=TABLE_NAME
    )

    #DAG
    create_table >> clear_table >> insert_top_locations

