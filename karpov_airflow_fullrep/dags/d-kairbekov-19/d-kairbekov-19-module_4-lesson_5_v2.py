"""
Module 4 Lesson 5 v2
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import ast
from datetime import datetime


import logging
from d_kairbekov_19_plugins.d_kairbekov_ram_top_location_operator import (
    DKairbekovRamTopLocationOperator,
)


DEFAULT_ARGS = {
    "start_date": days_ago(5),
    "owner": "d-kairbekov-19",
    "poke_interval": 600,
}


with DAG(
    "d-kairbekov-19-module_4-lesson_5_v2",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["d-kairbekov-19"],
) as dag:

    top_location = DKairbekovRamTopLocationOperator(
        task_id="top_location",
    )

    create_new_table = PostgresOperator(
        task_id="create_new_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
            CREATE TABLE IF NOT EXISTS public.d_kairbekov_19_ram_location
            (
                id INT4 NOT NULL,
                name VARCHAR(200) NOT NULL,
                type VARCHAR(100) NOT NULL,
                dimension VARCHAR(200) NOT NULL,
                resident_cnt INT4 NOT NULL
            )
            DISTRIBUTED BY (dimension);
        """,
    )

    def load_df_to_gp(**kwargs):
        logging.info(kwargs["templates_dict"]["df"])
        df_location = pd.DataFrame(
            ast.literal_eval(kwargs["templates_dict"]["df"]),
            columns=["id", "name", "type", "dimension", "resident_cnt"],
        )
        df_location = df_location.sort_values("resident_cnt", ascending=False).head(3)
        logging.info("Df create success")
        logging.info(df_location)

        pg_hook = PostgresHook(
            postgres_conn_id="conn_greenplum_write"
        )  # инициализируем хук
        pg_hook.run("TRUNCATE TABLE public.d_kairbekov_19_ram_location", True)
        logging.info("Truncate success")

        # Конвертируем в tuple для вставки
        df_location = [tuple(r) for r in df_location.to_numpy()]
        # или в лист df_location = df_location.values.tolist()
        pg_hook.insert_rows(
            "public.d_kairbekov_19_ram_location", df_location, commit_every=3
        )
        logging.info("Download success")

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_df_to_gp,
        templates_dict={"df": '{{ ti.xcom_pull(task_ids="top_location") }}'},
        provide_context=True,
    )

    top_location >> create_new_table >> load_data
