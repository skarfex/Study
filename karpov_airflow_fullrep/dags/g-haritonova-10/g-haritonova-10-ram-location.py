"""
даг использует кастомный оператор GHaritonovaRamLocationOperator, который грузит топ 3 локациий РиМ в гринплам
"""

from airflow import DAG
from airflow.utils.dates import days_ago
# import logging
# from datetime import datetime, timezone
# import pendulum
from g_haritonova_10_plugins.ram_location_operator import GHaritonovaRamLocationOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'g-haritonova-10',
    'start_date': days_ago(1),
    'poke_interval': 60
}

with DAG("g-haritonova-10-ram-location",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['g-haritonova-10']
          ) as dag:

    start = DummyOperator(task_id='start')

    finish = DummyOperator(task_id='finish')

    load_top_locations_to_csv = GHaritonovaRamLocationOperator(
        task_id='load_top_locations_to_csv',
        dag=dag
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY g_haritonova_10_ram_location FROM STDIN DELIMITER ','", '/tmp/g-haritonova-locations.csv')

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    start >> load_top_locations_to_csv >> load_csv_to_gp >> finish