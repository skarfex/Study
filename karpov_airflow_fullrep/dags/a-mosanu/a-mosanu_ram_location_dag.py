from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.edgemodifier import Label
from a_mosanu_plugins.a_mosanu_ram_location_operator import andr_ShowTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-mosanu',
    'poke_interval': 600
}

dag =  DAG( 'Show_top_locations_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-mosanu'] )


show_top_locations = andr_ShowTopLocationsOperator (
        task_id = 'show_top_locations',
        dag = dag)

dummy = DummyOperator(task_id="DummyOperator", dag = dag)

# # Define the task to write DataFrame to Greenplum
# write_to_greenplum_task = WriteDataFrameToGreenplumOperator(
#     task_id='write_to_greenplum',
#     pull_task_id = 'show_top_locations',
#     postgres_conn_id='conn_greenplum_write',
#     dag = dag,
#     xcom_push = True
# )

# SQL statement to insert records into the table
sql_create_table = """CREATE TABLE if not exists  a_mosanu_ram_location
        ( id text, name text,  type text,  dimension text, resident_cnt text) DISTRIBUTED BY (id);
        TRUNCATE TABLE a_mosanu_ram_location;"""

sql_write = """
    INSERT INTO a_mosanu_ram_location (id, name, type, dimension, resident_cnt)
    VALUES {{ ti.xcom_pull(task_ids='show_top_locations', key='written_dataframe') }};
"""

create_GP_table = PostgresOperator(
    task_id="prepare_gp_table",
    postgres_conn_id="conn_greenplum_write",
    sql=sql_create_table,
    dag = dag )

write_to_greenplum_task = PostgresOperator(
    task_id="load_to_gp",
    postgres_conn_id="conn_greenplum_write",
    sql= sql_write,
    dag = dag
)
end_task = DummyOperator(task_id='end_task', dag = dag)

dummy >> show_top_locations >> create_GP_table >> write_to_greenplum_task >> end_task

