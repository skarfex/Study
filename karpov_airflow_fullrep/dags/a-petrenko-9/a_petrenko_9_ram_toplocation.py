"""
DAG FOR GET TOP 3 LOCATION from Rick&Morty
"""
from airflow import DAG

from airflow.utils.dates import days_ago

from a_petrenko_9_plugins.a_petrenko_ram_get_top_location_operator import APetrenkoGetTopLocationOperator


DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-petrenko-9',
    'poke_interval': 600
}

with DAG("a-petrenko-9_ram_top_loc_dag",
         schedule_interval='@daily',
         catchup=False,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['ap-9']
         ) as dag:

    print_ram_top_loc = APetrenkoGetTopLocationOperator(
        task_id='print_ram_top_loc'
    )


    print_ram_top_loc






