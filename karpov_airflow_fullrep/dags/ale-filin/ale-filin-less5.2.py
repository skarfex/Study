from airflow.decorators import dag, task
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ale_filin_plugins.rnm_operator import RnMLocationsOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'ale-filin',
    'start_date': datetime(2022,1,1),
    'schedule_interval': None,
    'depends_on_past': False
}

with DAG(
    dag_id='ale-filin-less5.2',
    tags=['ale-filin'],
    description='Test dag for rick and morty api',
    default_args=default_args,
    catchup=False
) as dag:
    create_rnm_table = PostgresOperator(
        task_id='create_rnm_table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            create table if not exists ale_filin_rnm_location(
                    id				integer primary key,
                    name			text,
                    type			text,
                    dimension		text,
                    resident_cnt	integer,
                    op_time		timestamp default CURRENT_TIMESTAMP
            );
        """,
        autocommit = True
    )

    rnm_task = RnMLocationsOperator(task_id="gather-rnm-locations", top_limit = 3)

    populate_rnm_table = PostgresOperator(
        task_id='populate_rnm_table',
        postgres_conn_id='conn_greenplum_write',
        sql=[
            "TRUNCATE TABLE ale_filin_rnm_location",
            "INSERT INTO ale_filin_rnm_location VALUES {{ ti.xcom_pull(task_ids='gather-rnm-locations') }}",
        ],
        autocommit=True,
    )

    end = DummyOperator(task_id="end")

    create_rnm_table >> rnm_task >> populate_rnm_table >> end
