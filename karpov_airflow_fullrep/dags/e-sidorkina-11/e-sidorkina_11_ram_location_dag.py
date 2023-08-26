"""
Тестовый даг - задание к уроку 5
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from e_sidorkina_11_plugins.top_location import ESidorkinaRamLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'e-sidorkina-11',
    'poke_interval': 300
}

with DAG("e-sidorkina-11_ram_location_dags",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-sidorkina-11']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    topn_locations_to_csv = ESidorkinaRamLocationOperator(
        task_id='top_locations_to_csv',
        n=3,
        dag=dag
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY \"e_sidorkina_11_ram_location\" FROM STDIN DELIMITER ','",
                            '/tmp/e_sidorkina_11_ram_locations.csv')

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    def truncate_table_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.run("truncate table \"e_sidorkina_11_ram_location\"", False)

    truncate_table = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table_func,
        dag=dag
    )

    dummy >> topn_locations_to_csv >> truncate_table >> load_csv_to_gp

    dag.doc_md = __doc__
