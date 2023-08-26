"""
DAG тестовый ...
"""
import pandas as pd
import logging
import datetime as dt
#import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ИМПОРТ СВОЕГО ОПЕРАТОРА (ПУТЬ plugins/p_emeljanov/p-emeljanov_operator.py
#from plugins.p_emeljanov.p_emeljanov_operator import PveRamSpeciesCountOperator
# Operator - когда нужно одно поведение от нескольких тасков
# Есть несколько тасок, которые делают примерно одно и то же, но для них нужны разные параметры

DEFAULT_ARGS = {
    'owner': 'p_emeljanov',
    'poke_interval': 600
}

with DAG(dag_id="p-emeljanov_test2",
    schedule_interval='@daily',
    start_date=dt.datetime(2022, 3, 2),
    end_date=dt.datetime(2022, 3, 4),
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    #template_searchpath='/usr/local/airflow/include',
    tags=['p_emeljanov']
) as dag:

# Task_1
    start_empty = DummyOperator(task_id="empty")

# Task_2
# Создание таблицы  через Task(экземпляр PostgresOperator)
    sql_create_table_ram_location = '''
        CREATE TABLE if not exists public.p_emeljanov_test2 (
        id serial,
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
    # Вставка "ручных" данных. Через Task(экземпляр PostgresOperator)
    sql_insert_data_ram_location = '''
        INSERT INTO public.p_emeljanov_test2
        ("name", "type", dimension, resident_cnt) VALUES
        ('Rick','Local','Quad',7),
        ('Morty','Local','Quad',3),
        ('Ben','Local','Dual',15);
    '''
    insert_data_ram_location = PostgresOperator(
        task_id='insert_data_ram_location',
        postgres_conn_id='conn_greenplum_write',
        sql=sql_insert_data_ram_location
    )

# Task_4 УДАЧНО!
    # Вcтавка "данных из list". Через метод .insert_rows PostgresHook. потом вызов функции в Task(экземпляр PythonOperator)

    li = [('Ben_ins_fun_0_0', 'Local_ins_fun_0_1', 'Dual_ins_fun_0_2', 150),
          ('Ben_ins_fun_1_0', 'Local_ins_fun_1_1', 'Dual_ins_fun_1_2', 150)]

    # Функция вставки даных в таблицу GP из list (list передан как аргумент функции из контекста PythonOperator)
    def insert_list_ram_location_func(li):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.insert_rows(
            table='public.p_emeljanov_test2',
            target_fields=('name', 'type', 'dimension', 'resident_cnt'),
            rows=li
        )
    # Task
    insert_list_ram_location = PythonOperator(
        task_id='insert_list_ram_location',
        python_callable=insert_list_ram_location_func,
        op_args=[li]
    )

# Task_5
    # Вcтавка "данных из list". Функция.Через курсор PostgresHook. потом вызов функции в Task(экземпляр PythonOperator)

    list3_for_insert = [('Ben1_cur_ex', 'Local1_cur_ex', 'Dual1_cur_ex', 150),
                       ('Ben2_cur_ex', 'Local2_cur_ex', 'Dual2_cur_ex', 150)]

    def insert_list_cur_ram_location_func(li):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        for row in li:
            cursor.execute(f'''
                INSERT INTO public.p_emeljanov_test2
                (name, type, dimension, resident_cnt) VALUES {row};
            ''')
            logging.info(f'Результат запроса к GreenPlum: {row}')
        conn.commit()
        conn.close()

    insert_list_curr_ex = PythonOperator(
        task_id='insert_list_curr_ex',
        python_callable=insert_list_cur_ram_location_func,
        op_args=[list3_for_insert]
    )


# Task_6
    # Выборка данных через PostgresOperator
    select_data_ram_location = PostgresOperator(
        task_id='select_data_ram_location',
        postgres_conn_id='conn_greenplum_write',
        sql='SELECT * FROM public.p_emeljanov_test2;'
    )

# Task_7
    # Выборка данных через PythonOperator. Через PostgresHook потом вызов в Task(экземпляр PythonOperator)

    # Функция выборки даных из таблицы GP
    def select2_data_ram_location_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM public.p_emeljanov_test2;')
        query_res = cursor.fetchall()
        logging.info(f'Результат запроса к GreenPlum: {query_res}')
        conn.close()

    #Task
    select2_data_ram_location = PythonOperator(
        task_id='select2_data_ram_location',
        python_callable=select2_data_ram_location_func
    )

# Task_8 Выборка локаций из схемы Rick_Morty по API


start_empty >>\
create_table_ram_location >>\
insert_data_ram_location >> \
insert_list_ram_location >>\
insert_list_curr_ex >>\
select_data_ram_location >>\
select2_data_ram_location
            
            
