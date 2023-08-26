from a_lyan_plugins.ram_module import CountNumberResidentsOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

create_table_query = """
    CREATE TABLE IF NOT EXISTS a_lyan_ram_location (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    dimension VARCHAR NOT NULL,
    resident_cnt INT);
"""


DEFAULT_ARGS = {"owner": "a-lyan", "poke_interval": 600}

dag = DAG(
    dag_id="a-lyan-ram-location",
    start_date=days_ago(1),
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["lyan", "ram_lyan"],
)


# Dummy task
start = DummyOperator(dag=dag, task_id="start")

# Task to create table
create_table = PostgresOperator(
    dag=dag,
    task_id="create_table",
    postgres_conn_id="conn_greenplum_write",
    database="students",
    sql=create_table_query,
)


# Task to pull, process data and insert data
process_data = CountNumberResidentsOperator(task_id="process_top_3", k=3)

load_to_db = PostgresOperator(
    task_id="load_to_db",
    postgres_conn_id="conn_greenplum_write",
    database="students",
    sql=[
        """
    TRUNCATE TABLE a_lyan_ram_location
    """,
        """
           insert into public.a_lyan_ram_location values {{ti.xcom_pull(task_ids='process_top_3')}}
           """,
    ],
)

start >> create_table >> process_data >> load_to_db
