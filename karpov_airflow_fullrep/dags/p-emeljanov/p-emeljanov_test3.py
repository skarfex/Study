
'''
Запуск своего http hook
'''
#import plugins.p_emeljanov.p_emeljanov_hook
from p_emeljanov.p_emeljanov_hook import PveRickMortyHook
import logging
import datetime as dt
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'p_emeljanov',
    'poke_interval': 600
}

with DAG(dag_id="p-emeljanov_dag_test3",
    schedule_interval='@daily',
    start_date=dt.datetime(2022, 3, 2),
    end_date=dt.datetime(2022, 3, 3),
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    #template_searchpath='/usr/local/airflow/include',
    tags=['p_emeljanov']
) as dag:


### УРОК 5 ###

# Task_1
    start_empty = DummyOperator(task_id="empty")

# Task_2
    # Создание таблицы  через PostgresOperator
    sql_create_table_ram_location = '''
        CREATE TABLE if not exists public.p_emeljanov_ram_location_test3 (
        id serial,                                                  --!!!автоинкремент
        "name" varchar,
        "type" varchar,
        dimension varchar,
        resident_cnt varchar        
        );        
    '''
    create_table_ram_location = PostgresOperator(
        task_id='create_table_ram_location',
        postgres_conn_id='conn_greenplum_write',
        sql=sql_create_table_ram_location
    )


# Task_3
    # Обращение к API c вывод списка top3 'residents', генерация SQL запроса, запись данных кортежей в GreenPlum
    def get_list_top3_location_func():
        URL = 'https://rickandmortyapi.com/api/location'  # адрес API схемы 'location'
        result_get = requests.get(URL)                    # вернет python <class 'requests.models.Response'>
        result_get_json = result_get.json()               # конв. объект 'requests.models.Response' в объект - dict
        result_get_json_key1 = result_get_json['results']

# ВМЕСТО, СВОЙ  HOOK ------
#        hook = PveRickMortyHook(http_conn_id='epv_hook')
#        result_get_json_key1 = hook.get_locations()
#-------------------

        location_list = []
        for location in result_get_json_key1:
            location_dict = {
                'name': location['name'],
                'type': location['type'],
                'dimension': location['dimension'],
                'resident_cnt': len(location['residents'])
            }
            location_list.append(location_dict)
        sorted_locations = sorted(location_list,
                                  key=lambda cnt: cnt['resident_cnt'],
                                  reverse=True)
        top3_location = sorted_locations[:3]

        return top3_location

    def generate_sql_query_insert_top3_location_func():
        insert_values = get_list_top3_location_func()
        insert_values_loc = [f"('{loc['name']}', '{loc['type']}', '{loc['dimension']}', {loc['resident_cnt']})"
                     for loc in insert_values]
        insert_sql = f'''
            TRUNCATE TABLE public.p_emeljanov_ram_location_test3 RESTART IDENTITY; --!!!очистка таблицы и сброс счетчика id
            INSERT INTO public.p_emeljanov_ram_location_test3 
            (name, type, dimension, resident_cnt) VALUES 
            {','.join(insert_values_loc)};
        '''
        return insert_sql

    sql_query_insert_top3_location = generate_sql_query_insert_top3_location_func()

    insert_top3_location = PostgresOperator(
        task_id='insert_top3_location',
        postgres_conn_id='conn_greenplum_write',
        sql=sql_query_insert_top3_location
    )

### PIPELINE ###
start_empty >>\
create_table_ram_location >>\
insert_top3_location





