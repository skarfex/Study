""" (03) Автоматизация ETL-процессов >> 5 урок >> Задания

Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

"""

from airflow.models import DAG
from datetime import datetime
from i_derkach_16_plugins.operators import TopLocationsOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def update(table:str, **kwargs) -> None:
    """ Обновление таблицы в базе

    >>> ... table ~ (str) - таблица
    >>> return (None)
    """

    def insert(cursor, table:str, data:list) -> None:
        query = """insert into {} ({}) values {};""".format(
            table, 
            ', '.join(['"%s"']*len(data[0])) % tuple(data[0]), 
            ', '.join(['%s']*len(data))
        )
        cursor.execute(query, [tuple(x.values()) for x in data])

    hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = hook.get_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    ###
    
    data = kwargs['ti'].xcom_pull(
        key='i-derkach-16-03-05-toplocations',
        task_ids=['get_toplocations']
    )[0]
    
    cursor.execute(f"""
        create table if not exists {table} (
            id int, "name" varchar, "type" varchar, dimension varchar, residents_cnt int
        );
        truncate {table};
    """)

    insert(cursor, table, data)


with DAG(
    dag_id='i-derkach-16-03-05.0',
    start_date=datetime(2023, 1, 15),
    schedule_interval='@daily',
    tags=['i-derkach']
) as dag:

    TopLocationsOperator(
        task_id='get_toplocations'
    ) >> \
    PythonOperator(
        task_id='upload_toplocations',
        python_callable=update,
        provide_context=True,
        op_kwargs={"table": "public.i_derkach_16_ram_location"}
    )
