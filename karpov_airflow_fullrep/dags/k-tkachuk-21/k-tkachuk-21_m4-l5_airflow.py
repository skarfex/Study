"""
m4-l5_airflow
homework
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from k_tkachuk_21_plugins.k_tkachuk_21_ram_locations_operator import KTkachuk21RamLocationOperator


DEFAULT_ARGS = {
    'owner': 'k-tkachuk-21'
    , 'poke_interval': 600
    , 'start_date': days_ago(2)
}

dag = DAG(dag_id='k-tkachuk-21-m4-l5_airflow'
          , schedule_interval='@daily'
          , default_args=DEFAULT_ARGS
          , max_active_runs=1
          , tags=['k-tkachuk-21']
          )

table_creation = PostgresOperator(
    task_id='table_creation'
    , postgres_conn_id='conn_greenplum_write'
    , sql='''
        CREATE TABLE IF NOT EXISTS public.k_tkachuk_21_ram_location(
            id int NOT NULL PRIMARY KEY
            , name varchar(1024) NULL
            , type varchar(1024) NULL
            , dimension varchar(1024) NULL
            , resident_cnt int NULL
        )
        DISTRIBUTED BY (id);
        TRUNCATE TABLE public.k_tkachuk_21_ram_location;
        '''
    , autocommit=True
    , dag=dag
)

top_locations = KTkachuk21RamLocationOperator(task_id='top_locations'
                                              , num_of_locations=3
                                              , dag=dag
                                              )

table_creation >> top_locations
