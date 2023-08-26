"""
Getting location data from Rick and Morty API and inserting to greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import json
import requests

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'il-pastuhov',
    'poke_interval': 600
}

with DAG('il-pastuhov_ram_location',
    schedule_interval = '@daily',
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    tags = ['il-pastuhov']
) as dag:

    def load_location_from_api_func():
        r = requests.get('https://rickandmortyapi.com/api/location')

        # Конвертируем в json, запоминаем только значимую часть ответа
        locations = json.loads(r.text)['results']

        # Соберём локации в массив словарей
        location_list = []
        for location in locations:
            location_dict = {
                'id': location['id'],
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)

        # Отсортируем этот массив и выберем три первых элемента
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top3_location = sorted_locations[:3]

        # Соберём список значений для вставки в таблицу
        insert_values = [f"({loc['id']}, '{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                         for loc in top3_location]

        result = ','.join(insert_values)
        return result

    load_location_from_api = PythonOperator(
        task_id='load_location_from_api',
        python_callable=load_location_from_api_func,
        dag=dag
    )

    def create_greenplum_table_func():
        create_DDL = '''
        CREATE TABLE public."il-pastukhov_ram_location" (
            id varchar NULL,
            "name" varchar NULL,
            "type" varchar NULL,
            dimension varchar NULL,
            resident_cnt int4 NULL,
            CONSTRAINT il_pastukhov_ram_location_pk PRIMARY KEY (id)
        )
        DISTRIBUTED BY (id);'''
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(create_DDL)
        logging.info('Table is created')
        connection.commit()
        cursor.close()
        connection.close()

    create_greenplum_table = PythonOperator(
        task_id='create_greenplum_table',
        python_callable=create_greenplum_table_func,
        dag=dag
    )

    def truncate_greenplum_table_func():
        request = '''TRUNCATE TABLE public."il-pastukhov_ram_location"'''
        pg_hook = PostgresHook(postgres_conn_id="conn_greenplum_write")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        connection.commit()
        cursor.close()
        connection.close()

    truncate_greenplum_table = PythonOperator(
        task_id='truncate_greenplum_table',
        python_callable=truncate_greenplum_table_func,
        dag=dag
    )

    insert_into_greenplum = PostgresOperator(
        task_id='insert_into_greenplum',
        postgres_conn_id='conn_greenplum_write',
        sql='INSERT INTO public."il-pastukhov_ram_location" VALUES {{ ti.xcom_pull(task_ids="load_location_from_api") }}',
        autocommit=True
    )

    load_location_from_api >> create_greenplum_table >> truncate_greenplum_table >> insert_into_greenplum