"""
Top-3 locations with info on residents
"""
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from d_teplova_plugins.d_teplova_ram_location_operator import DTeplovaRamLocationOperator

DEFAULT_ARGS = {
    'owner': 'd-teplova',
    "start_date": days_ago(2),
    'poke_interval': 600
}

with DAG(
        "d-teplova-ram-location",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['d-teplova']
) as dag:

    top_location_pull=DTeplovaRamLocationOperator(
        task_id='top_location_pull'
    )

    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и курсор
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS public.d_teplova_ram_location
            (
                id INT4 NOT NULL,
                name VARCHAR(200) NOT NULL,
                type VARCHAR(100) NOT NULL,
                dimension VARCHAR(200) NOT NULL,
                resident_cnt INT4 NOT NULL
            )
            DISTRIBUTED BY (dimension);
            """
        )
        conn.commit()  # исполняем sql

    create_sql_table = PythonOperator(
        task_id='create_sql_table',
        python_callable=create_table
        )

    def df_to_sql(ti):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        pg_hook.run("TRUNCATE TABLE public.d_teplova_ram_location", True)

        top_loc_df = ti.xcom_pull(task_ids='top_location_pull', key='return_value')
        top_loc = top_loc_df.values.tolist()
        pg_hook.insert_rows(
            "public.d_teplova_ram_location", top_loc, commit_every=3
        )

    load_data_to_sql = PythonOperator(
        task_id='load_data_to_sql',
        python_callable=df_to_sql,
        provide_context=True
        )

    top_location_pull >> create_sql_table >> load_data_to_sql
