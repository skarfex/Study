"""
> Задание
1. Создайте в GreenPlum'е таблицу с названием "<ваш_логин>_ram_location" с полями id, name, type, dimension, resident_cnt.
2. С помощью API (https://rickandmortyapi.com/documentation/#location) найдите три локации сериала "Рик и Морти" с наибольшим количеством резидентов.
3. Запишите значения соответствующих полей этих трёх локаций в таблицу. resident_cnt — длина списка в поле residents.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from a_morshinin_plugins.a_morshinin_rnm_custom_operator import AleksandrMorshininRamTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-morshinin',
    'poke_interval': 600
}

with DAG("a-morshinin-ram-location",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-morshinin']
         ) as dag:
    def check_if_table_exists_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = " \
                        "'t_shinjaeva_ram_location'); "
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(sql_statement)
        is_exists = cursor.fetchone()[0]
        logging.info('--------------------------------')
        logging.info('Is table exists: ' + str(is_exists))
        logging.info('--------------------------------')
        if is_exists:
            return 'dummy'
        else:
            return 'create_table'


    check_if_table_exists = BranchPythonOperator(
        task_id='check_if_table_exists',
        python_callable=check_if_table_exists_func
    )

    def create_table_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = "CREATE TABLE IF NOT EXISTS t_shinjaeva_ram_location (id text, name text, type text, " \
                        "dimension text, resident_cnt text); "
        pg_hook.run(sql_statement, False)


    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_func
    )

    dummy = DummyOperator(task_id='dummy')

    get_top_locations = AleksandrMorshininRamTopLocationsOperator(
        task_id='get_top_locations',
        top_count=3,
        sort_parameter='resident_cnt',
        trigger_rule='one_success'
    )

    def add_locations_to_db_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute("SELECT * FROM t_shinjaeva_ram_location;")
        if cursor.fetchone():
            logging.info('--------------------------------')
            logging.info('Locations is already in table')
            logging.info(cursor.fetchone())
            logging.info('--------------------------------')
        else:
            logging.info('--------------------------------')
            logging.info(kwargs['task_instance'].xcom_pull(task_ids='get_top_locations', key='top_locations'))
            logging.info('--------------------------------')

            locations = kwargs['task_instance'].xcom_pull(task_ids='get_top_locations', key='top_locations')
            values = []
            for location in locations:
                values.append(f"('{location['id']}', '{location['name']}', '{location['type']}', '{location['dimension']}', '{location['resident_cnt']}')")
            sql_statement = "INSERT INTO t_shinjaeva_ram_location (id, name, type, dimension, resident_cnt) values " + ",".join(values)
            logging.info(sql_statement)
            pg_hook.run(sql_statement, False)

    add_locations_to_db = PythonOperator(
        task_id='add_locations_to_db',
        python_callable=add_locations_to_db_func
    )

    check_if_table_exists >> [create_table, dummy] >> get_top_locations >> add_locations_to_db
