"""
Lesson 5 dag
"""
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from ed_kuzhamuratov_17_plugins.ed_kuzhamuratov_17_ram_top3_locations import ed_kuzhamuratov_17_RamTop3LocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'ed-kuzhamuratov',
    'poke_interval': 600
}

with DAG("ed-kuzhamuratov_lesson5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ed-kuzhamuratov']
) as dag:

    start = DummyOperator(task_id="start")

    ram_top3_locations = ed_kuzhamuratov_17_RamTop3LocationsOperator(
        task_id='ram_top3_locations',
        dag=dag
    )

    end = DummyOperator(task_id="end")

    start >> ram_top3_locations >> end