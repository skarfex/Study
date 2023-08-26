"""
My Dag lesson 5 rickandmorty
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from s_jakovleva_20_plugins.s_jakovleva_20_ram_operator import SJakovleva20OperatorTopLocations


name_table = 's_jakovleva_20_ram_location'

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-jakovleva-20',
    'poke_interval': 600
}

with DAG("ys_dag_ram",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-jakovleva-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    create_table_gp = PostgresOperator(
        task_id='create_table_gp',
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql=
        f'''
        CREATE TABLE IF NOT EXISTS {name_table}
        (id INT,
        name VARCHAR,
        type VARCHAR,
        dimension VARCHAR,
        resident_cnt INT       
        )''',
        autocommit=True,
        dag=dag
    )

    truncate_table_gp = PostgresOperator(
        task_id='truncate_table_gp',
        postgres_conn_id='conn_greenplum_write',
        sql=
        f'''
        TRUNCATE TABLE {name_table}
        ''',
        autocommit=True,
        dag=dag
    )
    locations = SJakovleva20OperatorTopLocations(
        task_id='locations',
        conn_id='conn_greenplum_write',
        table_name=name_table,
        dag=dag
    )

    dummy >> create_table_gp >> truncate_table_gp >> locations