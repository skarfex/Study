from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
from mrjabushkin_plugins.mrjabushkin_lesson5_plug import MryabOper
from airflow.operators.postgres_operator import PostgresOperator


default_args={
    'owner':'m-rjabushkin',
    'retries':5,
    'retry_delay':timedelta(minutes=5),
    'start_date': days_ago(1), 
    'schedule_interval':'@daily'
}

with DAG (
    'm-rjabushkin_lesson_5',
    default_args=default_args,
    tags=['lesson_5']
) as dag:

    task1 = PostgresOperator(
        task_id='table',
        postgres_conn_id='conn_greenplum_write',
        sql="""
            create table if not exists public."m-rjabushkin_ram_location" (
                id integer unique not null,
                name text null,
                type text null,
                dimension text null,
                resident_cnt integer,
                unique(id, name, type, dimension, resident_cnt))
                distributed by (id);
            """
    )

    task2 = MryabOper(
        task_id='load_pg'
    )

    task3=PostgresOperator(
        task_id='check_table',
        postgres_conn_id='conn_greenplum_write',
        sql=
        """
        select * from public."m-rjabushkin_ram_location"
        """
    )


    task1 >> task2 >> task3
