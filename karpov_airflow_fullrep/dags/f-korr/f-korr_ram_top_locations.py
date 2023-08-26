from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from f_korr_plugins.f_korr_operator import KorrRaMTopLocationsOperator


DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'f-korr',
    'poke_interval': 300
}

with DAG('f-korr_ram_top_locations',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['f-korr', 'ht7o8KnE']
) as dag:

    start = DummyOperator(task_id='start', dag=dag)

    get_top_three_locations = KorrRaMTopLocationsOperator(
        task_id='get_top_three_locations',
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            CREATE TABLE IF NOT EXISTS f_korr_ram_location (
                id            INTEGER PRIMARY KEY,
                name          VARCHAR NOT NULL,
                type          VARCHAR NOT NULL,
                dimension     VARCHAR NOT NULL,
                resident_cnt  INTEGER);
            '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        sql='TRUNCATE TABLE f_korr_ram_location'
    )

    write_locations = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            INSERT INTO f_korr_ram_location VALUES 
            {{ ti.xcom_pull(task_ids='get_top_three_locations', key='return_value') }}
            '''
    )

    end = DummyOperator(task_id='end', dag=dag)

    start>>get_top_three_locations>>create_table>>clear_table>>write_locations>>end

