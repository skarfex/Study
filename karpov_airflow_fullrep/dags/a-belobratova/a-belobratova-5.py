"""
Создает в GreenPlum'е таблицу с названием "a_belobratova_ram_location" с полями id, name, type, dimension, resident_cnt (длина списка в поле residents)
С помощью API (https://rickandmortyapi.com/documentation/#location) находит три локации сериала "Рик и Морти" с наибольшим количеством резидентов
Записывает значения соответствующих полей этих трёх локаций в таблицу
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from a_belobratova_plugins.a_belobratova_r_m_operator import ABelobratovaRickMortyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-belobratova',
    'poke_interval': 600
}

with DAG(
    dag_id="a-belobratova-5",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-belobratova']
) as dag:

    # Создает в greenplum таблицу a_belobratova_ram_location
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''CREATE TABLE IF NOT EXISTS a_belobratova_ram_location (
                    id int,
                    name varchar,
                    type varchar,
                    dimension varchar,
                    resident_cnt int);
            '''
    )

    # Очищает данные  таблице a_belobratova_ram_location
    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE a_belobratova_ram_location;'
    )

    # Собственный оператор. Обращается к API. Возвращает список топ-3 локаций
    get_location_r_m = ABelobratovaRickMortyOperator(
        task_id='get_location_r_m'
    )

    # По словарю формирует строку для sql запроса insert: (3,  'name', ...)
    def dict_to_str_values(dict):
        return f"({dict['id']}, '{dict['name']}', '{dict['type']}', '{dict['dimension']}', {dict['resident_cnt']})"

    # Создает текстовый sql запрос
    def create_insert_sql_def(ti):
        top_locations = ti.xcom_pull(task_ids='get_location_r_m', key='return_value')
        sql = 'INSERT INTO a_belobratova_ram_location (id, name, type, dimension, resident_cnt)\n'
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

    create_table >> clear_table >> get_location_r_m >> create_insert_sql >> insert_top_3_locations
