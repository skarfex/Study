from a_kovalev_8_plugins.a_kovalev_8_RAM_plugin import KovalevRAMOperator
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime

from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-kovalev-8',
    'poke_interval': 60

}

with DAG("kovalev-8_ram_to_gp",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-kovalev-8']
         ) as dag:
    start = DummyOperator(task_id='start')

    Load_from_api = KovalevRAMOperator(
        task_id='Load_from_api',
        row_count=3
    )


    def print_xcom_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['templates_dict']['implicit'])
        logging.info('--------------')


    print_xcom = PythonOperator(
        task_id='print_xcom',
        python_callable=print_xcom_func,
        templates_dict={'implicit': '{{ ti.xcom_pull(task_ids="Load_from_api") }}'},
        provide_context=True
    )


    def insert_to_gp_func(**kwargs):
        api_data = kwargs['ti'].xcom_pull(task_ids='Load_from_api', key='return_value')
        logging.info(f'xcom pull , {api_data}')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        logging.info('--------------')
        logging.info('connection get')
        logging.info('--------------')
        cur = conn.cursor()
        cur.execute("delete from a_kovalev_8_ram_location")
        for location in api_data:
            cur.execute(f"""
                            insert into public.a_kovalev_8_ram_location 
                            values({location[0]}
                                , '{location[1]}'
                                , '{location[2]}'
                                , '{location[3]}'
                                , {location[4]}); 
                          """)
        conn.commit()
        logging.info('insertion done')


    Load_to_GP = PythonOperator(
            task_id='Load_to_GP',
            python_callable=insert_to_gp_func,
            #templates_dict={'implicit': '{{ ti.xcom_pull(task_ids=Load_from_api) }}'},
            provide_context=True
    )


    start >> Load_from_api >> print_xcom >> Load_to_GP
