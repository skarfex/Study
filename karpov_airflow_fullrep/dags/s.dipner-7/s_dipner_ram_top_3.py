from datetime import datetime as dt
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from s_dipner_plugins.s_dipner_ram_3_top_operator import Sdipner_RaM_location_operator

DEFAULT_ARGS = {
    'start_date': dt(year=2022, month=10, day=15),
    'end_date': dt(year=2022, month=10, day=18),
    'owner': 's.dipner-7',
    'poke_interval': 600
}

with DAG("s.dipner-7_top3_locations",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov', 's.dipner-7']
          ) as dag:

    start = DummyOperator(task_id='start')

    table_create = PostgresOperator(
        task_id="table_create",
        postgres_conn_id='conn_greenplum_write',
        database = "karpovcourses",
        sql = """
        create table if not exists "s_dipner_7_ram_location" (
            id           integer primary key,
            name         varchar(256),
            type         varchar(256),
            dimension    varchar(256),
            resident_cnt integer
        ) distributed by (id);""",
        autocommit=True
    )

    get_top3_location = Sdipner_RaM_location_operator(
        task_id='get_top3_location'
    )

    load_top3_locations_to_gp = PostgresOperator(
        task_id='load_top3_locations_to_gp',
        postgres_conn_id='conn_greenplum_write',
        database = "karpovcourses",
        sql=[
            "truncate table s_dipner_7_ram_location",
            """
            insert into 
                s_dipner_7_ram_location
            values
            {{ ti.xcom_pull(task_ids='get_top3_location') }}
            """,
        ],
        autocommit=True
    )

    end = DummyOperator(task_id='end')

start >> table_create >> get_top3_location >> load_top3_locations_to_gp >> end