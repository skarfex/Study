from airflow import DAG
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from m_evgeniia_21_plugins.m_evgeniia_21_location_operator import MEvgeniia21TopLocationsOperator

from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-evgeniia-21'
}


@dag(
    dag_id='m-evgeniia-21_less5_ram',
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-evgeniia-21']
)
def generate_dag():
    start = DummyOperator(task_id='start')

    get_top_locations = MEvgeniia21TopLocationsOperator(
        task_id='get_top_locations'
    )

    start >> get_top_locations


dag = generate_dag()
