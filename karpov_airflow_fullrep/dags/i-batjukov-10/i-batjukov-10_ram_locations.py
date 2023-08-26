"""
Igor Batyukov

Load RaM top locations with max number of residents to GreenPlum

"""

from airflow import DAG
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from i_batjukov_10_plugins.i_batjukov_10_ram_locations_operator import IBatjukov10RamLocationsOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 6, 1),
    'end_date': datetime(2022, 7, 30),
    'owner': 'i-batjukov-10',
    'retries': 1,
    'poke_interval': 600
}

csv_path = '/tmp/ram.csv'

with DAG("i-batjukov-10_ram_locations",
         schedule_interval='@weekly',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-batjukov-10']
         ) as dag:

    load_top_locations_to_csv = IBatjukov10RamLocationsOperator(
        task_id='load_top_locations_to_csv',
        num_of_locations=3,
        execution_dt='{{ ds }}'
    )


    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("TRUNCATE TABLE i_batjukov_10_ram_location;", False)
        pg_hook.copy_expert("COPY i_batjukov_10_ram_location FROM STDIN DELIMITER ',';", csv_path)


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func
    )

    remove_csv = BashOperator(
        task_id='remove_csv',
        bash_command=f'rm {csv_path}'
    )
    load_top_locations_to_csv >> load_csv_to_gp >> remove_csv
