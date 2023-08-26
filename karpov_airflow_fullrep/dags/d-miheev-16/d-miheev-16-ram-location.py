import logging
import pendulum as pdl

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from d_miheev_16_plugins.d_miheev_16_ram_plugin import d_miheev_16_ram_plugin


@dag(
    dag_id='d-miheev-16-ram-location',

    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False,

    tags=['d-miheev-16'],

    # наследуются всеми задачами
    default_args={
        'start_date': pdl.today('UTC').add(days=-2),
        'owner': 'd-miheev-16',
        'poke_interval': 600,
    },
)
def pipeline():
    #
    # Переменные
    #
    conn = 'conn_greenplum_write'
    table = 'd_miheev_16_ram_location'

    create = PostgresOperator(
        task_id='create',
        postgres_conn_id=conn,
        sql=f'''create table if not exists {table} (
                  id serial primary key,
                  name varchar(100) not null,
                  type varchar(100) not null,
                  dimension varchar(100) not null,
                  resident_cnt int4 not null
                ) distributed by (id);
        ''', autocommit=True,
    )

    clear = PostgresOperator(
        task_id='clear',
        postgres_conn_id=conn,
        sql=f'truncate {table};',
        autocommit=True,
    )

    insert = d_miheev_16_ram_plugin(
        task_id='insert',
        conn_id=conn,
        table_name=table,
        loc_limit=3,
    )

    check = PostgresOperator(
        task_id='check',
        postgres_conn_id=conn,
        sql=f'select * from {table};',
        autocommit=True,
    )


    #
    # Конвейер
    #
    create >> clear >> insert >> check


# Зарегистрировать себя
dag = pipeline()
