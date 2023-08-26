"""
Даг для задания 5: выгрузить топ-3 локаций по количеству резидентов и загрузить их в greenplum
"""
from airflow import DAG
from e_shulga_plugins.ram_location_operator import EShulgaRAMLocationOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-shulga',
    'poke_interval': 600
}

with DAG("e-shulga_ram_api",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-shulga']
         ) as dag:

    upload_locations = EShulgaRAMLocationOperator(
        task_id='upload_locations',
        n=3,
        dag=dag
    )

    upload_locations