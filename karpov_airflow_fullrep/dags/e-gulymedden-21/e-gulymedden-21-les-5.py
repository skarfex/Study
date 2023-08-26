"""
Урок 5
Задание
"""
from airflow import DAG
import logging
from datetime import datetime
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from e_gulymedden_21_plugins.e_gulymedden_21_les_5_get_locations import Les_5_get_locations

DEFAULT_ARGS = {
    'owner': 'e-gulymedden-21',
    'start_date': days_ago(2),
    'poke_interval': 600
}

with DAG("e-gulymedden-21-les-5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-gulymedden-21']
         ) as dag:

    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    insert_top_3_location = Les_5_get_locations(
        task_id='insert_top_3_location',
    )

    start >> insert_top_3_location
