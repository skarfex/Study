"""
ram
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import json

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from v_laptev_plugins.v_laptev_ram import VLaptevLocationsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-laptev',
    'poke_interval': 600
}

with DAG("v-laptev-ram",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-laptev']
         ) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    get_locations = VLaptevLocationsOperator(
        task_id = 'get_locations',
        number_of_locations = 3
    )

    def write_locations_func(**kwargs):
        locations = kwargs['templates_dict']['implicit']
        logging.info("---------------------")
        logging.info("Locations from XCom:")
        logging.info(locations)
        logging.info("---------------------")
        locations = json.loads(locations.replace("'", "\""))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        #cursor.execute('drop table if exists public.v_laptev_ram_location;')
        #conn.commit()
        cursor.execute('create table if not exists public.v_laptev_ram_location (id integer primary key, "name" varchar(50), "type" varchar(50), dimension varchar(50), resident_cnt integer) distributed by (id);')
        cursor.execute('truncate table public.v_laptev_ram_location;')
        for location in locations:
            logging.info(f"len: '{len(location)}'")
            logging.info(f"type: '{type(location)}'")
            logging.info(location)
            logging.info(f"id: '{location['id']}'")
            logging.info(f"name: '{location['name']}'")
            logging.info(f"type: '{location['type']}'")
            logging.info(f"dimension: '{location['dimension']}'")
            logging.info(f"resident_cnt: '{location['resident_cnt']}'")
            cursor.execute(f"insert into public.v_laptev_ram_location values ('{location['id']}', '{location['name']}', '{location['type']}', '{location['dimension']}', '{location['resident_cnt']}');")
        conn.commit()


    write_locations = PythonOperator(
        task_id = 'write_locations',
        python_callable = write_locations_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="get_locations") }}'},
        provide_context=True
    )

    start >> get_locations >> write_locations >> end

    