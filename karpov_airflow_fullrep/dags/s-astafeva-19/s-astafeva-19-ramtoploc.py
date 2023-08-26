"""
Даг, который записывает топ локаций в таблицу greenplum

Задание:
- Создать в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" 
с полями id, name, type, dimension, resident_cnt.
- С помощью API (https://rickandmortyapi.com/documentation/#location) найти три локации с наибольшим количеством резидентов.
- Записать значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.

Условия:
- Использовать соединение 'conn_greenplum_write'
- Предпочтительно использовать свой оператор для вычисления топ-3 локаций
- Можно использовать хук PostgresHook, можно оператор PostgresOperator
- Можно использовать XCom для передачи значений между тасками или сразу записывать нужное значение в таблицу
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from s_astafeva_19_plugins.s_astafeva_19_ram_topresloc import SAstafeva19RAMResLocOperator


DEFAULT_ARGS: dict = {
    'start_date': days_ago(2),
    'owner': 's-astafeva-19',
    'poke_interval': 600,
}

PG_TABLE: str = 's_astafeva_19_ram_location'
CONN: str = 'conn_greenplum_write'

with DAG ("s-astafeva-19-ramtoploc",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['s-astafeva-19']
         ) as dag:

    create_ram_pgtable = PostgresOperator(
        task_id='create_ram_pgtable',
        postgres_conn_id=CONN,
        sql=f"""
            CREATE TABLE IF NOT EXISTS public.{PG_TABLE} (
                id INT PRIMARY KEY,
                name VARCHAR,
                type VARCHAR,
                dimension VARCHAR,
                resident_cnt INT
                );
        """,
        autocommit=True
    )

    clear_ram_pgtable = PostgresOperator(
        task_id='clear_ram_pgtable',
        postgres_conn_id=CONN,
        sql=f"TRUNCATE TABLE public.{PG_TABLE};",
        autocommit=True
    )

    get_top_locations_from_ram_api = SAstafeva19RAMResLocOperator(
        task_id='get_top_locations_from_ram_api'
    )

    insert_top_locations_to_ram_pgtable = PostgresOperator(
        task_id='insert_top_locations_to_ram_pgtable',
        postgres_conn_id=CONN,
        sql=f"INSERT INTO public.{PG_TABLE} VALUES " + \
            "{{ ti.xcom_pull(key='top_loc') }}" + ";"
        ,
        autocommit=True
    )

    create_ram_pgtable >> clear_ram_pgtable >> get_top_locations_from_ram_api >> insert_top_locations_to_ram_pgtable

