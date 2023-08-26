# -*- coding: utf-8 -*-


from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import logging

from v_peterfeld_plugins.v_peterfeld_ram_location_operator import VpeterfeldGetTopRamLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-peterfeld',
    'poke_interval': 300
}

with DAG('v-peterfeld_task_5',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-peterfeld','task_5']) as dag:

    get_top_3_loc = VpeterfeldGetTopRamLocationsOperator(
        task_id='get_top_3_loc',
        locations_number=3
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
                CREATE TABLE IF NOT EXISTS v_peterfeld_ram_location (
                    id        integer CONSTRAINT firstkey PRIMARY KEY,
                    name       varchar,
                    type       varchar,
                    dimension  varchar,
                    resident_cnt integer);
        '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='DELETE FROM v_peterfeld_ram_location;'
    )

    write_location = PostgresOperator(
        task_id='write_location',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        INSERT INTO v_peterfeld_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_3_loc', key='return_value') }}
        '''
    )


    get_top_3_loc >> create_table >> clear_table >> write_location