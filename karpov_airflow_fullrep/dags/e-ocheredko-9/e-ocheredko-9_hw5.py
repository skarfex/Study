"""
Даг для урока 5
"""
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from e-ocheredko-9_plugins.e-ocheredko-9-hw5_plugins import TopLocOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-ocheredko-9',
    'poke_interval': 600
}

dag = DAG(
    dag_id="e-ocheredko-9_rickandmorty",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-ocheredko-9']
)

start = DummyOperator(
    task_id='start',
    dag=dag
)


top_locations_to_gp = TopLocOperator(
    task_id='top_locations_to_gp',
)

finish = DummyOperator(
    task_id='finish',
    dag=dag
)


start >> top_locations_to_gp >> finish
