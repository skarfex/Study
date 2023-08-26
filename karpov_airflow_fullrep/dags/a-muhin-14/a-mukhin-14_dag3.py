'''
Get top 3 locations
'''

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


from a_mukhin_14_plugins.a_mukhin_14_location import RickMortyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'a-mukhin-14',
    'poke_interval': 600
}

dag = DAG("a-mukhin-14_dag3",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-mukhin-14']
          )


start = DummyOperator(task_id='start', dag=dag)


create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    sql='''
        CREATE TABLE IF NOT EXISTS public.a_mukhin_14_location (
            id            INTEGER PRIMARY KEY,
            name          VARCHAR NOT NULL,
            type          VARCHAR NOT NULL,
            dimension     VARCHAR NOT NULL,
            resident_cnt  INTEGER
        );
        TRUNCATE TABLE public.a_mukhin_14_location;
        ''',
    autocommit=True,
    dag=dag
)

insert_top_locations = RickMortyOperator(task_id='find_locations', dag=dag)


start >> create_table >> insert_top_locations
