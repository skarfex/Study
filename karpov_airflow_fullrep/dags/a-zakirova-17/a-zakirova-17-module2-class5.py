"""
Module 2 Class 5

"""

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from a_zakirova_17_plugins.a_zakirova_ram_plugin import A_Zakirova_17_Top3Locations

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-zakirova-17',
    'retries': 3
}

with DAG("a-zakirova-17_module2-class5",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-zakirova-17']
         ) as dag:

    dummy_start = DummyOperator(task_id="dummy_start")
    dummy_next = DummyOperator(task_id="dummy_next")

    def is_empty_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        select_query = f'select * from a_zakirova_17_ram_location'
        cursor.execute(select_query)
        if cursor.rowcount == 0:
            result = 'dummy_next'
        else:
            result = 'delete_rows'
        return result

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
            CREATE TABLE IF NOT EXISTS a_zakirova_17_ram_location (
                      id INTEGER PRIMARY KEY,
                      name VARCHAR,
                      type VARCHAR,
                      dimension VARCHAR,
                      residents_cnt INTEGER);
        ''',
        autocommit=True,
    )

    is_empty = BranchPythonOperator(
        task_id='is_empty',
        python_callable=is_empty_func
    )

    delete_rows = PostgresOperator(
        task_id='delete_rows',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql=f'TRUNCATE TABLE a_zakirova_17_ram_location',
        autocommit=True,
    )

    get_locations = A_Zakirova_17_Top3Locations(
        task_id='get_locations',
        trigger_rule='all_done',
        top_num=3
    )

    insert_locations = PostgresOperator(
        task_id='insert_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
            INSERT INTO a_zakirova_17_ram_location VALUES 
            {{ ti.xcom_pull(task_ids='get_locations', key='return_value') }}
            ''',
        autocommit=True,
    )

    dummy_start >> create_table >> is_empty >> [dummy_next, delete_rows] >> get_locations >> insert_locations
