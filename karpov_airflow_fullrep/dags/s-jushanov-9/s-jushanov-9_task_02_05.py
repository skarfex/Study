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

from s_jushanov_9_plugins.s_jushanov_9_ram_operator import SJushanov9RamOperator


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's-jushanov-9',
    'poke_interval': 600,
    'catchup': False
}


TABLE_NAME = 's_jushanov_9_ram_location'


with DAG(
    's_jushanov_02_05_task',
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jushanov-9']
) as dag:

    start_task = DummyOperator(
        task_id='start_task',
    )

    end_task = DummyOperator(
        task_id='end_task',
    )

    get_ram_locations = SJushanov9RamOperator(
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
            INSERT INTO {TABLE_NAME} (id, name, type, dimension, resident_cnt)
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
