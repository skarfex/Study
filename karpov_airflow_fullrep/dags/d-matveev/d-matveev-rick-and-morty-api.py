"""
Searching for top-3 locations in Rick And Morty TV series!
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from d_matveev_plugins.d_matveev_toplocations import DMatveevTopLocationsOperator

TABLE_NAME = 'd_matveev_ram_location'
CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd_matveev',
    'poke_interval': 600
}

dag = DAG("d-matveev-rick-and-morty-api",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['d-matveev']
          )

create_table = PostgresOperator(
    task_id='create_table',
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id text,
            name text,
            type text,
            dimension text,
            resident_cnt text,
            CONSTRAINT {TABLE_NAME}_pkey PRIMARY KEY (id, name)
            );
    """,
    autocommit=True,
    postgres_conn_id=CONN_ID,
    dag=dag
)

insert_top_locations = DMatveevTopLocationsOperator(
    task_id='export_top_locations',
    top_n=3,
    conn_id=CONN_ID,
    table_name=TABLE_NAME,
    dag=dag
)

create_table >> insert_top_locations