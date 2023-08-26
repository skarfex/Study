"""
Даг, который записывает в БД GreenPlum
три локации сериала "Рик и Морти" с наибольшим
количеством резидентов из https://rickandmortyapi.com/api/location.
Состоит из думми-оператора,
питон-оператора (создает таблицу в БД, если ее еще нет)

"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from s_bayandina_plugins.s_bayandina_ram_location_operator import SBayandinaRamLocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-bayandina',
    'poke_interval': 600
}

dag = DAG("s-bayandina_dag_5_1",
          schedule_interval= '@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-bayandina']
          )

dummy=DummyOperator(task_id='dummy',
                    dag=dag
                    )

def check_DBtable_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute("select exists (select * from information_schema.tables where table_name = 's_bayandina_ram_location' and table_schema = 'public') as table_exists;")  # исполняем sql
    query_res = cursor.fetchall()[0][0]
    if not query_res:
        sql_statement="create table s_bayandina_ram_location(id integer, name character varying(40), type character varying(40), dimension character varying(40), resident_cnt integer);"
        pg_hook.run(sql_statement, False)
        logging.info("Table 's_bayandina_ram_location' is created")
    else:
        logging.info("Table 's_bayandina_ram_location' exists")


python_DBtable_check = PythonOperator(task_id='python_DBtable_check',
                            python_callable=check_DBtable_func,
                            dag=dag
                            )

locations_load = SBayandinaRamLocationOperator(task_id='locations_load',
                            dag=dag
                            )


dummy >> python_DBtable_check >> locations_load