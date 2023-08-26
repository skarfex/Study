# """
# Script to count species by type from Rick & Morty!
# """
#
#
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.operators.dummy_operator import DummyOperator
# import logging
# import requests
# from airflow import AirflowException
# from airflow.models import BaseOperator
# from airflow.hooks.http_hook import HttpHook
# from airflow.hooks.postgres_hook import PostgresHook
# from m_scherbinin_2_plugins.m_scherbinin_ram_operator import MarselRamSpeciesCountOperator
#
#
# DEFAULT_ARGS = {
#     'start_date': days_ago(1),
#     'owner': 'm-scherbinin-2',
#     'poke_interval': 600
# }
#
# with DAG("m_scherbinin_ram_location",
#          schedule_interval='@daily',
#          default_args=DEFAULT_ARGS,
#          max_active_runs=1,
#          tags=['m-scherbinin-2-Rick-Morte']
#          ) as dag:
#
#     probe_dummy_2 = DummyOperator(
#             task_id='probe_dummy',
#             trigger_rule='one_success',
#             dag=dag
#         )
#
#     print_alien_count = MarselRamSpeciesCountOperator(
#         task_id='print_alien_count',
#         species_type='Alien',
#         dag=dag
#     )
#
#     probe_dummy_2 >> print_alien_count


"""
1. Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location"
с полями id, name, type, dimension, resident_cnt.

2. С помощью API (https://rickandmortyapi.com/documentation/#location) 
найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.

3. Запишите значения соответствующих полей этих трёх локаций в таблицу.
resident_cnt — длина списка в поле residents.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from m_scherbinin_2_plugins.m_scherbinin_ram_operator import MarselRamSpeciesCountOperator



DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'm-scherbinin-2',
    'poke_interval': 600,
    'catchup': False
}


TABLE_NAME = 'm_scherbinin_2_ram_location'


with DAG("m_scherbinin_ram_location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-scherbinin-2-Rick-Morte']
         ) as dag:

    start_task = DummyOperator(
        task_id='start_task',
    )

    end_task = DummyOperator(
        task_id='end_task',
    )

    get_ram_locations = MarselRamSpeciesCountOperator(
        task_id='get_ram_locations',
        top_count=3,
        do_xcom_push=True
    )

    create_ram_table = PostgresOperator(
        task_id='create_ram_table',
        postgres_conn_id='conn_greenplum_write',
        sql=f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
	            id integer PRIMARY KEY,
	            name varchar(1024) NULL,
	            type varchar(1024) NULL,
	            dimension varchar(1024) NULL,
	            resident_cnt integer NULL
            ) DISTRIBUTED BY (id);
            TRUNCATE TABLE {TABLE_NAME};
        """,
        autocommit=True
    )

    def save_ram_locations_func(task_instance):
        values_list = task_instance.xcom_pull(
            task_ids='get_ram_locations',
            key='return_value'
        )

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        query_str = f"""
            INSERT INTO m_scherbinin_2_ram_location (id, name, type, dimension, resident_cnt)
            VALUES (%s, %s, %s, %s, %s)
        """
        for values in values_list:
            logging.info(f"VALUES: {str(values)}")
            cursor.execute(query_str, values)
        conn.commit()
        cursor.close()
        conn.close()

    save_ram_locations = PythonOperator(
        task_id='save_ram_locations',
        python_callable=save_ram_locations_func
    )

    start_task >> get_ram_locations >> create_ram_table >> save_ram_locations >> end_task


start_task.doc_md = """Начало DAG"""
get_ram_locations.doc_md = """Получаем локации"""
create_ram_table.doc_md = """Создаем таблицу для записи локаций"""
save_ram_locations.doc_md = """Сохраняем локации"""
end_task.doc_md = """Окончание DAG"""
