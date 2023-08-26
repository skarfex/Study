"""
Даг, находит топ n локаций по количеству персонажей residents в Rick&Morty и записывает в таблицу GreenPlum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from e_pogrebetskaja_10_plugins.ram_location import EPogrebetskayaRamLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'e-pogrebetskaja-10',
    'poke_interval': 300
}

with DAG("e-pogrebetskaja-10_ram_location_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-pogrebetskaja-10']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    topn_locations_to_csv = EPogrebetskayaRamLocationOperator(
        task_id='topn_locations_to_csv',
        n=3,
        dag=dag
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.copy_expert("COPY \"e-pogrebetskaja-10_ram_location\" FROM STDIN DELIMITER ','",
                            '/tmp/e-pogrebetskaja-10_ram_locations.csv')

    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )

    def truncate_table_func():
        pg_hook = PostgresHook('conn_greenplum_write')
        pg_hook.run("truncate table \"e-pogrebetskaja-10_ram_location\"", False)

    truncate_table = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table_func,
        dag=dag
    )

    dummy >> topn_locations_to_csv >> truncate_table >> load_csv_to_gp

    dag.doc_md = __doc__
