from airflow import DAG
from datetime import datetime
from p_pirogov_plugins.p_pirogov_ram_top_loc_operator import PirogovOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
import logging

DEFAULT_ARGS = {
    'start_date': datetime(2023, 4, 5),
    'end_date': datetime(2023, 5, 1),
    'owner': 'p-pirogov',
    'poke_interval': 300
}

with DAG(
        dag_id="p_pirogov_lesson_5",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-pirogov']
) as dag:
    table = 'p_pirogov_ram_location'
    conn = 'conn_greenplum_write'

    get_top3_location = PirogovOperator(
        task_id='get_top3_location',
        count_top_locations=3,
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=conn,
        trigger_rule='all_success',
        sql=f'''
        CREATE TABLE IF NOT EXISTS {table} (
                    id INT4 PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR NOT NULL,
                    resident_cnt INT4 NOT NULL);
        '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id=conn,
        trigger_rule='all_done',
        sql=f'TRUNCATE TABLE {table}'
    )

    def insert_locations_data(**context):
        pg_hook = PostgresHook('conn_greenplum_write')
        locations = context['ti'].xcom_pull(
            task_ids='get_top3_location', key='p_pirogov_locations'
        )
        logging.info(f'{locations}')
        for location in locations:
            logging.info(f'{location}')
            pg_hook.run(
                sql=f"""
                INSERT INTO public.{table} 
                VALUES (
                    {location[0]}, 
                    '{location[1]}', 
                    '{location[2]}', 
                    '{location[3]}', 
                    '{location[4]}'
                );
                """
            )


    insert = PythonOperator(
        task_id='insert',
        python_callable=insert_locations_data,
        provide_context=True
    )

    check = PostgresOperator(
        task_id='check',
        postgres_conn_id=conn,
        sql=f'select * from {table};',
        autocommit=True,
    )

get_top3_location >> create_table >> clear_table >> insert >> check
