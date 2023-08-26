import logging
from airflow import DAG
from airflow.utils.dates import days_ago

from a_mashkarin_6_plugins.a_mashkarin_6_plugin import top_3_location

from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    'owner': 'a-mashkarin-6',
    'start_date': days_ago(2),
    'poke_interval': 300
}
dag = DAG('a-mashkarin-6_less_5',
          schedule_interval='@hourly',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-mashkarin-6'])

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql='''create table if not exists a_mashkarin_6_ram_location (
                    id text,
                    name text,
                    type text,
                    dimension text,
                    resident_cnt text);
        ''',
    autocommit=True,
    dag=dag
)

clear_table = PostgresOperator(
    task_id='clear_table',
    postgres_conn_id='conn_greenplum_write',
    sql="truncate table a_mashkarin_6_ram_location",
    autocommit=True,
    dag=dag
)

to_gp = top_3_location(
    task_id='to_gp',
    dag=dag
)

create_table >> clear_table >> to_gp
