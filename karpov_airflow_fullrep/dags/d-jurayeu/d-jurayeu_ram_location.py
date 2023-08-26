"""
Даг для создания таблицы в GP с 3 локациями сериала "Рик и Морти" с наибольшим количеством резидентов
"""

from airflow.decorators import dag, task
from datetime import datetime
import logging

from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from d_jurayeu_plugins.d_jurayeu_top_locations_operator import DJurayeuTopLocationsOperator

DEFAULT_ARGS = {
    'owner': 'd-jurayeu',
    'start_date': days_ago(2)
}

@dag(
    "d-jurayeu_ram_location",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-jurayeu']
)
def d_jurayeu_ram_location_taskflow():

    @task
    def create_table():
        sql_statement = """CREATE TABLE IF NOT EXISTS d_jurayeu_ram_location (
                        id int PRIMARY KEY,
                        name varchar NOT NULL,
                        type varchar NOT NULL,
                        dimension varchar NOT NULL, 
                        resident_cnt int NOT NULL);"""
        sql_clear_table = """TRUNCATE TABLE d_jurayeu_ram_location;"""
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run(sql_statement, False)
        pg_hook.run(sql_clear_table, False)

    @task
    def get_top_3_locations(*args):
        top_3_locations = DJurayeuTopLocationsOperator(task_id='get_top_3_locations')
        return top_3_locations.execute()

    @task
    def insert_locations_to_gp(top_3_locations):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        sql_statement = []
        for location in top_3_locations:
            location_id = location.get('id')
            location_name = location.get('name')
            location_type = location.get('type')
            location_dimension = location.get('dimension')
            resident_cnt = location.get('resident_cnt')
            statement = f"""
                        INSERT INTO d_jurayeu_ram_location
                        VALUES ({location_id},'{location_name}','{location_type}','{location_dimension}',{resident_cnt})
                        """
            sql_statement.append(statement)
        pg_hook.run(sql_statement, False)

    insert_locations_to_gp(get_top_3_locations(create_table()))


d_jurayeu_ram_location_taskflow_dag = d_jurayeu_ram_location_taskflow()
