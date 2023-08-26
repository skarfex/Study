from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from f_niyazova_plugins.f_niyazova_ram_top3_location import FNiyazovaRamTop3LocationOperator

''' 1.С помощью API (https://rickandmortyapi.com/documentation/#location) 
        находим три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
    2. Создаем в GreenPlum'е таблицу с названием "f-niyazova_ram_location", если она не существует,
     с полями id, name, type, dimension, resident_cnt.
    3. Если таблица пустая -> записываем значения, 
       если таблица содержит данные -> очищаем, записываем новые значения
'''

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'f-niyazova',
    'poke_interval': 600,
}

with DAG('f-niyazova_ram_location',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['f-niyazova'],
    catchup=True
    
) as dag:

    get_top3_location = FNiyazovaRamTop3LocationOperator(
        task_id='get_top3_location')

    
    def create_or_update_table_func(**kwargs):
        ti = kwargs['ti']
        values = ti.xcom_pull(task_ids = 'get_top3_location', key='return_value')
        list_of_values =[]
        table_name = 'public.f_niyazova_ram_location'
        for value in values:
            temp_values = f"({value['id']}, '{value['name']}', '{value['type']}', '{value['dimension']}', {value['resident_cnt']})"
            list_of_values.append(temp_values)

        sql_create = f'''CREATE TABLE IF NOT EXISTS {table_name} (
            id              INTEGER PRIMARY KEY,
            name            VARCHAR(200),
            type            VARCHAR(200),
            dimension       VARCHAR(200),
            resident_cnt    INTEGER
            )
            DISTRIBUTED BY (id);

            TRUNCATE TABLE {table_name};

            '''

        sql_insert = f'INSERT INTO {table_name} VALUES {",".join(list_of_values)}'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_create)
        cursor.execute(sql_insert)
        conn.commit()
        conn.close()


    create_or_update_table = PythonOperator(
        task_id='create_or_update_table',
        python_callable=create_or_update_table_func
        )

    get_top3_location >> create_or_update_table