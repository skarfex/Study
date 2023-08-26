"""
Тестовый даг, который состоит из сложный связей между тасками
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-filatova-20',
    'poke_interval': 600
}

with DAG("fil_test3",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-filatova-20']
         ) as dag:
    t1 = DummyOperator(task_id="task1"),
    t2 = DummyOperator(task_id="task2"),
    t3 = DummyOperator(task_id="task3"),
    t4 = DummyOperator(task_id="task4"),
    t5 = DummyOperator(task_id="task5"),
    t6 = DummyOperator(task_id="task6"),
    t7 = DummyOperator(task_id="task7")

    [t1, t2] >> t5
    [t2, t4, t3] >> t6
    [t5, t6, t3] >> t7



