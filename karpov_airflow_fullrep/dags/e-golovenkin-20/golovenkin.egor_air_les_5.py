"""
Dag 3 with custom operator (Airflow, les 5)
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from e_golovenkin_plugins.E_Golovenkin_20_Get_Top_Locations import E_Golovenkin_20_Get_Top_Locations
from airflow.operators.dummy import DummyOperator

import logging
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'golovenkin.egor',
    'provide_context' : True
}

with DAG('e-golovenkin-20-les5',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
    tags=['gol']
) as dag:

    start = DummyOperator(task_id='start')

    top_3_loc_to_gp = E_Golovenkin_20_Get_Top_Locations(
        task_id='top_3_loc_to_gp'
    )

    start >> top_3_loc_to_gp
