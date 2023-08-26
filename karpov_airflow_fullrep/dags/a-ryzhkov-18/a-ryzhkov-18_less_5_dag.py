"""
Собираем топ 3 локации по вселенной Рика и Морти
"""
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from a_ryzhkov_18_plugins.a_ryzhkov_location_top_operator import RyzhkovFindeTopLocationOperator
from a_ryzhkov_18_plugins.func_task import logging_done_update


DEFAULT_ARGS = {
    'owner': 'a-ryzhkov-18',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'priority_weight': 10,
    'start_date': datetime(2023, 3, 13),
    'end_date': datetime(2023, 4, 13),
    'sla': timedelta(minutes=7),
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule': 'all_success',
    'schedule_interval': '0 19 25 * *'
}

with DAG(dag_id='a-ryzhkov-18_less_5_dag',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-ryzhkov-18']) as dag:

    create_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
             CREATE TABLE IF NOT EXISTS a_ryzhkov_18_ram_location (
                         id INT4 PRIMARY KEY,
                         name VARCHAR NOT NULL,
                         type VARCHAR NOT NULL,
                         dimension VARCHAR NOT NULL,
                         resident_cnt INT4 NOT NULL);
             '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='TRUNCATE TABLE a_ryzhkov_18_ram_location'
    )

    update_date = RyzhkovFindeTopLocationOperator(
        task_id='get_top_3_locations',
        top_position=3
    )

    last_task_valid_correct_update = PythonOperator(
        task_id='return_result',
        python_callable=logging_done_update
    )

    create_table_task >> clear_table >> update_date >> last_task_valid_correct_update
