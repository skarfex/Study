from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from g_epifanov_plugins.g_epifanov_l5_ram_operator import g_epifanov_top_location_operator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'g-epifanov',
    'poke_interval': 600,
    'retries': 2,
    'schedule_interval' : '@once',
    'catchup':False
    }

with DAG('g-epifanov_ram_top_locations',
         default_args = DEFAULT_ARGS,
         tags = ['g-epifanov', 'l5'],
         ) as dag:

    start = DummyOperator(task_id = 'start', dag = dag)

    top_3_location = g_epifanov_top_location_operator(task_id = 'top_3_location')

    sql = [
    '''
    CREATE TABLE IF NOT EXISTS "g-epifanov_ram_location"
    ("id" int, "name" varchar, "type" varchar, "dimension" varchar, "residents_cnt" int)
    DISTRIBUTED BY (id)'''
    ,'''TRUNCATE TABLE "g-epifanov_ram_location"'''
    ,'''INSERT INTO "g-epifanov_ram_location" VALUES {{ ti.xcom_pull(task_ids = "top_3_location") }}'''
    ]

    load_data_to_gp = PostgresOperator(task_id = 'load_data_to_gp',
                                       postgres_conn_id = 'conn_greenplum_write',
                                       sql = sql)

    end = DummyOperator(task_id = 'end', dag = dag)

    start >> top_3_location >> load_data_to_gp >> end
