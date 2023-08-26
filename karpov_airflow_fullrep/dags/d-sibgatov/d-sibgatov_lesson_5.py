from airflow import DAG
from airflow.utils.dates import days_ago
from d_sibgatov_plugins.d_sibgatov_ram_top_loc_operator import DSibgatovRAMOperator
from airflow.operators.postgres_operator import PostgresOperator


DEFAULT_ARGS = {
    'start_date' : days_ago(2),
    'owner' : 'd-sibgatov',
    'poke_interval' : 300
}

with DAG("d-sibgatov_lesson_5",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-sibgatov']
) as dag:

    get_top_location = DSibgatovRAMOperator(
        task_id = 'get_top_location',
        count_top_locations=3,
    )


    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        CREATE TABLE IF NOT EXISTS d_sibgatov_ram_location (
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
        sql='TRUNCATE TABLE d_sibgatov_ram_location'
    )

    write_locations = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        INSERT INTO d_sibgatov_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_location', key='return_value') }}
        '''
    )

get_top_location >> create_table >> clear_table >> write_locations