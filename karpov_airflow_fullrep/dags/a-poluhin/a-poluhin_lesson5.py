from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

from a_poluhin_plugins.a_poluhin_ram_plugin import TopLocationsOperator
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-poluhin',
    'poke_interval': 120
}

dag = DAG('a-poluhin_lesson_5',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-poluhin'])

get_top_3_locations = TopLocationsOperator(
        task_id='get_top_3_locations',
        dag=dag)

create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='conn_greenplum_write',
        sql='''
        CREATE TABLE IF NOT EXISTS a_poluhin_ram_location (
                    id TEXT,
                    name TEXT,
                    type TEXT,
                    dimension TEXT,
                    resident_cnt TEXT);
        ''',
        dag=dag
    )

remove_all_lines = PostgresOperator(
        task_id='remove_duplicates',
        postgres_conn_id='conn_greenplum_write',
        sql="TRUNCATE TABLE a_poluhin_ram_location",
        dag=dag
    )

write_top_locations = PostgresOperator(
        task_id='write_top_locations',
        postgres_conn_id='conn_greenplum_write',
        sql='''
        INSERT INTO 
            a_poluhin_ram_location (id, name, type, dimension, resident_cnt) 
        VALUES 
            {{ ti.xcom_pull(task_ids='get_top_3_locations', key='return_value') }};
        ''',
        dag=dag
    )

def locations_to_logs_func():
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("a_poluhin_lesson5_conn")  # и именованный (необязательно) курсор
    cursor.execute('SELECT id, name, type, dimension, resident_cnt FROM a_poluhin_ram_location')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    logging.info(query_res)

log_our_locations = PythonOperator(
    task_id='log_our_locations',
    python_callable=locations_to_logs_func,
    dag=dag
)

get_top_3_locations >> create_table >> remove_all_lines >> write_top_locations >> log_our_locations
