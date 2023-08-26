"""
Леонид Ефремов
Задание к уроку 5. Разработка своих плагинов.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import logging

from l_efremov_plugins.l_efremov_ram_top_locations import LEfremovRamLocationResidentCountOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'owner': 'l-efremov',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': days_ago(2)
}


def gp_check_func(**kwargs):
    """Check if values already exist in table"""
    data = kwargs['ti'].xcom_pull(task_ids='collect_data', key='return_value')
    assert len(data) <= 3  # should only contain top 3 locations

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("data_collector")
    cursor.execute("SELECT name FROM students.public.l_efremov_ram_location")
    query_res = cursor.fetchall()
    logging.info(query_res)

    existed_names = [i['name'] for i in cursor]
    data_unique = data.loc[~data['name'].isin(existed_names)]
    kwargs['ti'].xcom_push(value=data_unique, key='return_value')


def gp_write_func(**kwargs):
    """Write to GreenPlum table, which already exist"""
    data = kwargs['ti'].xcom_pull(task_ids='collect_data', key='return_value')
    assert len(data) <= 3  # should only contain top 3 locations


    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    for _, row in data.iterrows():
        sql_statement = "INSERT INTO {table} VALUES ({id}, '{name}', '{type}', '{dmsn}', {rcnt});"\
                        .format(table='students.public.l_efremov_ram_location', id=row['id'], name=row['name'],
                                type=row['type'], dmsn=row['dimension'], rcnt=row['resident_cnt'])
        pg_hook.run(sql_statement)
        logging.info(sql_statement)


with DAG(
        dag_id="ds_task2",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        schedule_interval='@daily',
        tags=['l-efremov']
) as dag:
    begin = DummyOperator(task_id='init')

    collect_data = LEfremovRamLocationResidentCountOperator(
        task_id='collect_data'
    )

    gp_check = PythonOperator(
        task_id='check_gp',
        python_callable=gp_check_func,
        provide_context=True
    )

    gp_write = PythonOperator(
        task_id='write_to_gp',
        python_callable=gp_write_func,
        provide_context=True
    )

    begin >> collect_data >> gp_check >> gp_write
