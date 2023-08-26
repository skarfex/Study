"""
Dag for fifth lesson, where we use BranchOperator and plugins for our task.
We search for top-3 locations in cartoon 'Rick and Mortey'.
"""

from airflow import DAG
import psycopg2
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from a_gunicheva_16_plugins.a_gunicheva_16_Top3Location import GunichevaTopLocationsOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'a-gunicheva-16'
}

dag = DAG("a-gunicheva_16-lesson-5",
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-gunicheva-16']
          )

get_top_3_locations = GunichevaTopLocationsOperator(
        task_id='get_top_3_locations',
        top_number=3,
        dag=dag
    )

def is_table_exists():
    get_sql = f"select * from a_gunicheva_16_ram_location"

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("cursor_name")  # и именованный (необязательно) курсор
    try:
        cursor.execute(get_sql)
        return 'clear_table'
    except psycopg2.errors.UndefinedTable:
        print("таблица не существует, можно создать новую")
        locked = True
        return 'all_ok'
   
    

exists_branch = BranchPythonOperator(
        task_id='exists_branch',
        python_callable=is_table_exists,
        dag=dag)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='conn_greenplum_write',
    trigger_rule='one_success',
    sql='''
      create table if not exists 
      a_gunicheva_16_ram_location (
                  id SERIAL4 PRIMARY KEY,
                  name VARCHAR NOT NULL,
                  type VARCHAR NOT NULL,
                  dimension VARCHAR NOT NULL,
                  residents_cnt INT4 NOT NULL);
      ''',
    dag=dag
)

all_ok = DummyOperator(task_id='all_ok', dag=dag)

clear_table = PostgresOperator(
        task_id='clear_table',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_done',
        sql='drop table a_gunicheva_16_ram_location',
        dag=dag
    )

write_locations = PostgresOperator(
        task_id='write_locations',
        postgres_conn_id='conn_greenplum_write',
        trigger_rule='all_success',
        sql='''
        insert into a_gunicheva_16_ram_location VALUES 
        {{ ti.xcom_pull(task_ids='get_top_3_locations', key='return_value') }}
        ''',
        dag=dag
    )

get_top_3_locations >> exists_branch >> [all_ok, clear_table] >> create_table >>  write_locations
