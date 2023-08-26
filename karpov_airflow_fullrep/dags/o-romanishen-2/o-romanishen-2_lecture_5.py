from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from o_romanishen_2_plugins.o_romanishen_2__top_page import ApiTopPageOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'o-romanishen-2',
    'poke_interval': 300
}

with DAG('o-romanishen-2_lesson_5',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-romanishen-2']) as dag:

    get_top_3_position = ApiTopPageOperator(
        task_id='get_top_3_position',
        page_number=3
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        CREATE TABLE IF NOT EXISTS o_romanishen_2_ram_location (
                    id SERIAL4 PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR NOT NULL,
                    resident_cnt INT4 NOT NULL);
        '''
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='TRUNCATE TABLE o_romanishen_2_ram_location'
    )

    write_info = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''INSERT INTO o_romanishen_2_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_3_position', key='return_value') }}
        '''
    )


    get_top_3_position >> create_table >> truncate_table >> write_info