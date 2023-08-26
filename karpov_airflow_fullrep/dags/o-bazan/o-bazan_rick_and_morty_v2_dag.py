'''
Даг для подсчета количества инопланетян в сериале (реализация через кастомный оператор)
'''

from airflow import DAG
from airflow.utils.dates import days_ago

from o_bazan_plugins.o_bazan_species_count_operator import OBazanSpeciesCountOperator

DEFAULT_ARGS = {
    'owner': 'o-bazan',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id = "o-bazan_rick_and_morty_v2_dag",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    tags=['lesson5', 'Rick and Morty', 'o-bazan']
)

print_alien_count = OBazanSpeciesCountOperator(
    task_id='print_alien_count',
    species_type='Alien',
    dag=dag
)

print_alien_count