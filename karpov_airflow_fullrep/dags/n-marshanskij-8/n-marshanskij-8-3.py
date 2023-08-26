from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from n_marshanskij_8_plugins.n_marshanskij_8_locations import MarshanskijTopLocationsOperator


TABLE_NAME = 'n_marshanskij8_ram_location'
CONN_ID = 'conn_greenplum_write'

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'marshanskij',
    'poke_interval': 600
}

dag = DAG("n-marshanskij-8-3",
          schedule_interval= None,
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['marshanskij']
          )

create_table = PostgresOperator(
    task_id='create_table',
    sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL4 PRIMARY KEY,
            name VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            dimension VARCHAR NOT NULL,
            resident_cnt INT4 NOT NULL);
    """,
    autocommit=True,
    postgres_conn_id=CONN_ID,
    dag=dag
)

truncate_table = PostgresOperator(
    task_id='truncate_table',
    postgres_conn_id=CONN_ID,
    sql=f"TRUNCATE TABLE {TABLE_NAME};",
    autocommit=True,
    dag=dag
)

export_top_locations = MarshanskijTopLocationsOperator(
    task_id='export_top_locations',
    top_n=3,
    conn_id=CONN_ID,
    table_name=TABLE_NAME,
    dag=dag
)

create_table >> truncate_table >> export_top_locations