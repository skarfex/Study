from t_egorenkova_18_plugins.t_egorenkova_18_oprerator import TEgorenkovaOperator
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import logging

#
DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 't-egorenkova-18',
    'retries': 1,
    'poke_interval': 10
}

with DAG('t-egorenkova-18-parser',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['t-egorenkova-18']) as dag:

    start = DummyOperator(task_id="start")

    resident_cnt = TEgorenkovaOperator(
        task_id='resident_cnt',
        do_xcom_push=True
    )

    table_create = PostgresOperator(
        task_id="table_create",
        postgres_conn_id='conn_greenplum_write',
        database="students",
        sql="""
        create table if not exists "t_egorenkova_ram_location" (
            id           integer primary key,
            name         varchar(256),
            type         varchar(256),
            dimension    varchar(256),
            resident_cnt integer
        ) distributed by (id);""",
        autocommit=True
    )


    def write_result(ti):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(f'values_ {ti.xcom_pull(task_ids="resident_cnt", key="return_value")}')
        values = ti.xcom_pull(task_ids="resident_cnt", key="return_value")
        query = f'INSERT INTO t_egorenkova_ram_location VALUES (%s, %s, %s, %s, %s)'
        cursor.executemany(query, values)
        conn.commit()

    write_data = PythonOperator(
        task_id='write_data',
        provide_context=True,
        python_callable=write_result,
        do_xcom_push=True
    )

    finish = DummyOperator(task_id="finish")

    start >> resident_cnt >> table_create >> write_data >> finish


