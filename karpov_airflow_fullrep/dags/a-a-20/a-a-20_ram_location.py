from airflow import DAG
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from a_a_20_plugins.a_a_20_ram_hook import aa20_LocationOperator


create_table_query = """
    CREATE TABLE IF NOT EXISTS a_a_20_ram_location (
    id INT4 PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    type VARCHAR(100) NOT NULL,
    dimension VARCHAR(200) NOT NULL,
    resident_cnt INT4 NOT NULL);
"""


DEFAULT_ARGS = {"owner": "a-a-20", "poke_interval": 600}


def load_to_gp(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write') 
    pg_hook.run("TRUNCATE TABLE public.a_a_20_ram_location", True)
    logging.info("Truncate success")
    list_tuple_insert_rows = kwargs["templates_dict"]["data_to_load"]
    logging.info('data_to_load:', list_tuple_insert_rows)
    logging.info('data_to_load (type):', type(list_tuple_insert_rows))
    pg_hook.insert_rows(
        table = 'public.a_a_20_ram_location'
        , rows = list_tuple_insert_rows
        , commit_every=3
        , replace = True
        , replace_index='id'
        , target_fields = ['id', 'name', 'type', 'dimension', 'resident_cnt'])
    logging.info("Download success")


with DAG(
    dag_id='a-a-20_ram_location',
    start_date=days_ago(1),
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-a-20', 'ram', 'ram_location']
) as dag:

    # Dummy task
    start_DummyOperator = DummyOperator(dag=dag, task_id="start")

    # Task to create table
    create_table = PostgresOperator(
        dag=dag,
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        database="students",
        sql=create_table_query,
    )

    # Task to pull and process data 
    get_data = aa20_LocationOperator(task_id="get_data_top_3_location",)

    # Task to load data to gp
    load_to_db = PostgresOperator(
        task_id="load_to_db",
        postgres_conn_id="conn_greenplum_write",
        database="students",
        sql=["TRUNCATE TABLE public.a_a_20_ram_location"
            ,"insert into public.a_a_20_ram_location values {{ ti.xcom_pull(task_ids='get_data_top_3_location')}}; COMMIT;",  
        ],
    )


    # load_data = PythonOperator(
    #     task_id="load_data",
    #     python_callable=load_to_gp,
    #     templates_dict={"data_to_load": '{{ ti.xcom_pull(task_ids="get_data_top_3_location")}}'},
    #     provide_context=True,
    # )
    
    start_DummyOperator >> create_table >> get_data >> load_to_db