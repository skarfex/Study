from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from a_nedikov_plugins.a_nedikov_ram_location import RaMOperator

DEFAULT_ARGS = {
    'owner': 'a-nedikov',
    'start_date': days_ago(2),
    'retries': 3,
    'poke_interval': 600
}

with DAG(
    dag_id='a-nedikov_dag3',
    default_args=DEFAULT_ARGS,
    description='ram_locations',
    schedule_interval='@once',
    max_active_runs=1,
    tags=['a-nedikov'],
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        CREATE TABLE IF NOT EXISTS a_nedikov_ram_location (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR NOT NULL,
                    resident_cnt INTEGER NOT NULL);
        '''
    )

    trunc_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='TRUNCATE TABLE a_nedikov_ram_location'
    )

    insert_locations = RaMOperator(task_id='find_locations', dag=dag)

    create_table >> trunc_table >> insert_locations