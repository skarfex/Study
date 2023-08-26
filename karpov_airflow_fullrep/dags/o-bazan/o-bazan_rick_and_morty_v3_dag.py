'''
Даг для подсчета количества мертвых или живых персонажей в сериале (реализация через кастомный оператор)
'''

from airflow import DAG
from airflow.utils.dates import days_ago

from o_bazan_plugins.o_bazan_dead_or_alive_count_operator import OBazanDeadOrAliveCountOperator

DEFAULT_ARGS = {
    'owner': 'o-bazan',
    'start_date': days_ago(2)
}

dag = DAG(
    dag_id = "o-bazan_rick_and_morty_v3_dag",
    schedule_interval=None,
    default_args=DEFAULT_ARGS,
    tags=['lesson5', 'Rick and Morty', 'o-bazan']
)

print_dead_count = OBazanDeadOrAliveCountOperator(
    task_id='print_dead_count',
    dead_or_alive='Dead',
    dag=dag
)

print_dead_count