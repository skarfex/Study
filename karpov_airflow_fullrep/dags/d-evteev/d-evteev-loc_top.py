"""
Загрузка локаций
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from d_evteev_plugins.location_operator import RickMortyFindTopLocOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-evteev',
    'poke_interval': 600
}

with DAG("d-evteev-loc_top",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['d-evteev'],
         ) as dag:
    start = DummyOperator(task_id="start")

    prepare_table = PostgresOperator(
        task_id="prepare_table",
        postgres_conn_id='conn_greenplum_write',
        sql="""
        CREATE TABLE IF NOT EXISTS d_evteev_ram_location 
            (
                id int PRIMARY KEY, 
	            name varchar(256), 
	            type varchar(256), 
	            dimension varchar(256), 
	            resident_cnt int); 
	    truncate table d_evteev_ram_location;""")

    top_3_location = RickMortyFindTopLocOperator(
        task_id='top_3_location',
        target=3
    )

    start >> prepare_table >> top_3_location
