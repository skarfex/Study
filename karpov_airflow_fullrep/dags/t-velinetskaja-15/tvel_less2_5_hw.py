"""
Используется собственный плагин
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from t_velinetskaja_15_plugins.tvel_rm_topn import TVelRMTopNOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 't-velinetskaja',
    'poke_interval': 600
}

with DAG("tvel_less2_5_hw",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['velinetskaja', 'less2_5']
         ) as dag:
    start = DummyOperator(task_id='start')

    top_n_to_gp = TVelRMTopNOperator(
        task_id='top_n_to_gp',
        n=3,
        tbl_nm='t_velinetskaja_15_ram_location'
    )

    start >> top_n_to_gp
