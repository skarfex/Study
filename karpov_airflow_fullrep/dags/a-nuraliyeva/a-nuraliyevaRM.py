"""
Задание
Создайте в GreenPlum'е таблицу a_nuraliyeva_ram_location
С помощью API (https://rickandmortyapi.com/documentation/#location)
найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу.
resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

from a_nuraliyeva_plugins.ram_location import ANuraliyevaRamLocationOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-nuraliyeva',
    'poke_interval': 600
}

with DAG("a-nuraliyevaRM",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-nuraliyeva']
         ) as dag:
    start = DummyOperator(
        task_id='start'
    )

    load_to_csv = ANuraliyevaRamLocationOperator(
        task_id='load_to_csv',
        dag=dag
    )

    def load_csv_to_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY \"a_nuraliyeva_ram_location\"  FROM STDIN DELIMITER ','", '/tmp/a-nuraliyevaRM.csv')


    load_csv_to_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=load_csv_to_gp_func,
        dag=dag
    )


    def truncate_table_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("truncate table \"a_nuraliyeva_ram_location\"", False)


    truncate_table = PythonOperator(
        task_id='truncate_table',
        python_callable=truncate_table_func,
        dag=dag
    )

    start >> load_to_csv >> truncate_table >> load_csv_to_gp
