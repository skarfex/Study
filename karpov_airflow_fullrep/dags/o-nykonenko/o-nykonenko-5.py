"""
С помощью API (https://rickandmortyapi.com/documentation/#location) находит три локации сериала "Рик и Морти" с наибольшим количеством резидентов
Записывает значения соответствующих полей этих трёх локаций в таблицу
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from o_nykonenko_plugins.o_nykonenko_rm_operator import ONykonenkoRickMortyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'o-nykonenko',
    'poke_interval': 600
}

with DAG(
    dag_id="o-nykonenko-5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-nykonenko']
) as dag:


    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE o_nykonenko_ram_location;'
    )

    # Собственный оператор. Обращается к API. Возвращает список топ-3 локаций
    get_location_r_m = ONykonenkoRickMortyOperator(
        task_id='get_location_r_m'
    )

    # По словарю формирует строку для sql запроса insert: (3,  'name', ...)
    def dict_to_str_values(dict):
        return f"({dict['id']}, '{dict['name']}', '{dict['type']}', '{dict['dimension']}', {dict['resident_cnt']})"

    # Создает текстовый sql запрос
    def create_insert_sql_def(ti):
        top_locations = ti.xcom_pull(task_ids='get_location_r_m', key='return_value')
        sql = 'INSERT INTO o_nykonenko_ram_location (id, name, type, dimension, resident_cnt)\n'
        sql_values = ',\n'.join(map(dict_to_str_values, top_locations))
        sql += 'VALUES ' + sql_values + ';'
        logging.info(sql)
        return sql

    create_insert_sql = PythonOperator(
        task_id='create_insert_sql',
        python_callable=create_insert_sql_def
    )

    # Выполняет запрос из task_id = create_insert_sql
    insert_top_3_locations = PostgresOperator(
        task_id='insert_top_3_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql="{{ti.xcom_pull(task_ids='create_insert_sql', key='return_value')}}"
    )

    clear_table >> get_location_r_m >> create_insert_sql >> insert_top_3_locations
