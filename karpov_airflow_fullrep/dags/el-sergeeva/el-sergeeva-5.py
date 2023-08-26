"""
Даг урок 4

Создает таблицу
Очищает таблицу, если такая уже была
получает список локаций
записывает топ 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from el_sergeeva_plugins.el_sergeeva_operator import El_sergeeva_operator
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'el-sergeeva',
    'poke_interval': 600
}
with DAG("el-sergeeva-5",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['el-sergeeva']
) as dag:


    start = DummyOperator(task_id="start")

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        CREATE TABLE IF NOT EXISTS el_sergeeva_ram_location (
                    id SERIAL4 PRIMARY KEY,
                    name VARCHAR NOT NULL,
                    type VARCHAR NOT NULL,
                    dimension VARCHAR ,
                    resident_cnt INT4 NOT NULL);
        '''
    )

    clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='TRUNCATE TABLE el_sergeeva_ram_location'
    )

    get_top_list = El_sergeeva_operator(
        task_id='get_top_list',
        count_top = 3
    )


    def load_list(**kwargs):
        locations = kwargs['ti'].xcom_pull(task_ids='get_top_list', key='return_value')
        logging.info(f'locations- {locations}')
        pg_con = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_con.insert_rows('el_sergeeva_ram_location', rows=locations, commit_every=3)


    insert_list_in_table = PythonOperator(
        task_id='insert_list_in_table',
        python_callable=load_list,
        trigger_rule='all_success',
        provide_context=True

    )
    end =DummyOperator (task_id = "end")

    start >> get_top_list >> create_table >> clear_table>> insert_list_in_table >> end