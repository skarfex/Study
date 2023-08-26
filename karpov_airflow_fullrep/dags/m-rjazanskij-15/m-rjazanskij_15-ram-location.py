from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from m_rjazanskij_15_plugins.ram_location_operator import RAMtop3LocationOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-rjazanskij-15',
    'poke_interval': 600
}

with DAG("rjazanskij_3_task_ram_dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['rjazanskij']
) as dag:


    def working_with_table(ti):
        result_info = ti.xcom_pull(key='return_value', task_ids='get_top_location')
        logging.info(f'Recieved result of API {result_info}')
        sql_create_query = """
        CREATE TABLE if not exists m_rjazanskiy_15_ram_location
        (
            id integer,
            name text,
            type text,
            dimension text,
            resident_cnt integer
        );"""
        sql_truncate_query = "TRUNCATE TABLE m_rjazanskiy_15_ram_location;"

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        with pg_hook.get_conn() as conn:
            logging.info('Created the connection_to_GP')
            cursor = conn.cursor()
            cursor.execute(sql_create_query)
            logging.info('Created table if not exists')
            cursor.execute(sql_truncate_query)
            logging.info('TRuncated table')

            for i in range(3):
                logging.info(f'Insert the {i} row')
                query = f"""
                INSERT INTO m_rjazanskiy_15_ram_location (id, name, type, dimension, resident_cnt)
                        VALUES (    {result_info['id'][list(result_info['id'].keys())[i]]}
                                  , '{result_info['name'][list(result_info['name'].keys())[i]]}'
                                  , '{result_info['type'][list(result_info['type'].keys())[i]]}'
                                  , '{result_info['dimension'][list(result_info['dimension'].keys())[i]]}'
                                  , {result_info['resident_cnt'][list(result_info['resident_cnt'].keys())[i]]}
                                );"""
                logging.info(f'prepared query:\n {query}')
                cursor.execute(query)
            conn.commit()

    start = DummyOperator(task_id='start')

    get_top_location = RAMtop3LocationOperator(
         task_id='get_top_location'
    )

    prepare_table = PythonOperator(
        task_id='prepare_table',
        python_callable=working_with_table,
        dag=dag
    )

    finish = DummyOperator(task_id='finish')

    start >> get_top_location >> prepare_table >> finish