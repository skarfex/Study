"""
DAG для выбора заголовка статьи по дню недели
"""
from datetime import datetime
from airflow import DAG
import logging
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from i_romanchev_21_plugins.IRomanchevRickAndMortyOperator import IRomanchevRickAndMortyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'i-romanchev-21'
}

with DAG("i-romanchev-21-RicKAndMorty-dag",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-romanchev-21']
         ) as dag:
    def create_table_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("ir_cursor_name_1")
        create_script = '''
        CREATE TABLE IF NOT EXISTS "i_romanchev_21_ram_location" (
            id INT, 
            name text, 
            type text, 
            dimension text, 
            resident_cnt INT
        );
        '''
        logging.info("SQL script is {}".format(create_script))
        cursor.execute(create_script)


    # create_table1 = PythonOperator(
    #   task_id="create_table",
    #  python_callable=create_table_func
    # )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id='conn_greenplum_write',
        sql=[
            '''
            CREATE TABLE IF NOT EXISTS "i_romanchev_21_ram_location" (
                id INT, 
                name text, 
                type text, 
                dimension text, 
                resident_cnt INT
        );
        '''
        ]
    )

    find_locations = IRomanchevRickAndMortyOperator(
        task_id="find_locations"
    )


    def truncate_and_insert_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        # top_locations = kwargs['find_locations']['implicit']
        top_locations = kwargs['ti'].xcom_pull(task_ids="find_locations", key="return_value")
        logging.info(top_locations)
        # cursor.execute('''TRUNCATE TABLE "i_romanchev_21_ram_location";''')
        pg_hook.run('TRUNCATE TABLE "i_romanchev_21_ram_location";')
        for location in top_locations:
            script = f"""\
            INSERT INTO i_romanchev_21_ram_location (id, name, type, dimension, resident_cnt)
            VALUES ('{location[1]}', '{location[2]}', '{location[3]}', '{location[4]}', '{location[0]}');
            """
            logging.info(f"SQL INSERT IS {script}")
            #cursor.execute(script)
            pg_hook.run(script)


    truncate_and_insert = PythonOperator(
        task_id="truncate_and_insert",
        python_callable=truncate_and_insert_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="find_locations") }}'},
        provide_context=True
    )

    create_table >> find_locations >> truncate_and_insert
