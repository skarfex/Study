# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from d_severinets_plugins.d_severinets_ram_top_loc_operator import SeverinetsGetTopRamLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-severinets',
    'poke_interval': 300
}

with DAG('d-severinets_lesson_5',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-severinets']) as dag:

    get_top_3_locations = SeverinetsGetTopRamLocationsOperator(
        task_id='get_top_3_locations',
        locations_number=3
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        CREATE TABLE IF NOT EXISTS d_severinets_ram_location (
                    id SERIAL4 PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR NOT NULL,
                    resident_cnt INT4 NOT NULL);
        '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='TRUNCATE TABLE d_severinets_ram_location'
    )

    write_locations = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        INSERT INTO d_severinets_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_3_locations', key='return_value') }}
        '''
    )


    get_top_3_locations >> create_table >> clear_table >> write_locations