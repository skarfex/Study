"""
aanisimov rick and morty task decision with self operator
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from a_anisimov_plugins.AanisimovRamTopLocOperator import AanisimovRamTopLocOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a.anisimov',
    'poke_interval': 600
}

with DAG(
    dag_id='a.anisimov_ram_self_oper',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.anisimov']) as dag:

    start = DummyOperator(task_id="start")
    top_loc_to_gp = AanisimovRamTopLocOperator(task_id='top_loc_to_gp')
    end = DummyOperator(task_id="end")
    
    start >> top_loc_to_gp >> end
