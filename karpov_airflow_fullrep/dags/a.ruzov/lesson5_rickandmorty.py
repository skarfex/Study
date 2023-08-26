from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago

from a_ruzov_plugins.a_ruzov_top_loc_operator import ARuzovGetTopRamLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a.ruzov',
    'poke_interval': 300
}

with DAG('a.ruzov_rick_and_morty',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a.ruzov']) as dag:

    get_top_3_locations = ARuzovGetTopRamLocationsOperator(
        task_id='get_top_3_locations',
        locations_number=3
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        CREATE TABLE IF NOT EXISTS a_ruzov_ram_location (
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
        sql='TRUNCATE TABLE a_ruzov_ram_location'
    )

    write_locations = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        INSERT INTO a_ruzov_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_3_locations', key='return_value') }}
        '''
    )


    get_top_3_locations >> create_table >> clear_table >> write_locations