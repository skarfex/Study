"""
Создаю Даг для оправки на проверку по заданию курса 5 УРОК РАЗРАБОТКА СВОИХ ПЛАГИНОВ:

-в GreenPlum'е таблицу с названием "p_pirogov_ram_location" с полями id, name, type, dimension, resident_cnt
предотвратите повторное создание таблицы


"""
from airflow import DAG
from datetime import datetime
import logging
from textwrap import dedent
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import p_pirogov_Top3LocationOperator

DEFAULT_ARGS = {

    'owner': 'p-pirogov',
    # 'email': ['pppirogov21@gmail.com'],
    'email_on_failure': True,
    'poke_interval': 60,
    'start_date': datetime(2023, 4, 5),
    'end_date': datetime(2023, 5, 1)
}
with DAG(
        dag_id='p_pirogov_top3_location',
        schedule_interval='0 0 * * *',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-pirogov']
) as dag:
    start = DummyOperator(task_id='start')


    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("create table if not exists p_pirogov_ram_location \
                    (id int, name text,type text,dimension text,resident_cnt int);", False)


    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='TRUNCATE TABLE d_sibgatov_ram_location'
    )

    start >> create_table >> clear_table