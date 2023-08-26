"""
Даг для Урока 5
    Получение топ-3 locations из rick_and_morty API и соранение данных в greenplum
"""

from airflow import DAG
import logging
from datetime import datetime

from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from va_borisov_plugins.va_borisov_ram_operator import VaBorisovRamOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': ' va-borisov'
}

with DAG("va-borisov_lesson_5",
        schedule_interval='@once',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['lesson_5']
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
        create table if not exists va_borisov_ram_location
        (
            id integer,
            name varchar,
            type varchar,
            dimension varchar,
            resident_cnt integer
        )
        distributed by (id);
        """
        )
    
    truncate_table = PostgresOperator(
        task_id="truncate_table",
        postgres_conn_id="conn_greenplum_write",
        sql="""
        truncate va_borisov_ram_location;
        """
        )
    
    ram_data = VaBorisovRamOperator(
        task_id="ram_data",
        url="https://rickandmortyapi.com/api/location"
    )

    
    create_table >> truncate_table >> ram_data