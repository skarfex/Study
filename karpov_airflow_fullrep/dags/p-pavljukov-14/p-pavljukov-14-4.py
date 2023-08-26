"""
Даг по поиску топ 3 локаций из сериала
"""
from typing import Any

import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from p_pavljukov_14_plugins.p_pavljukov_14_operator import PavljukovLocationsOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 11, 1, 10, 00),
    'owner': 'p-pavljukov-14',
    'poke_interval': 400
}

with DAG(
        dag_id='p-pavljukov-14-4',
        schedule_interval="0 10 * * 1-6",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-pavljukov-14']
        ) as dag:


    start_task = DummyOperator(task_id='start_task')

    found_top = PavljukovLocationsOperator(
        task_id = 'found_top',
        do_xcom_push=True
    )


    def write_csv_func(**kwargs):
        ti = kwargs['ti']
        top = ti.xcom_pull(key='top', task_ids='found_top')
        logging.info(top)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run('drop table if exists public.p_pavljukov_14_ram_location;')
        pg_hook.run('''create table if not exists public.p_pavljukov_14_ram_location
                              (id text, name text, type text, dimension text, resident_cnt text);''')
        location_list = [(i['id'], i['name'], i['type'], i['dimension'], i['resident_cnt'])
                         for i in top]
        pg_hook.insert_rows(
            table='p_pavljukov_14_ram_location',
            rows=location_list,
            target_fields=['id', 'name', 'type', 'dimension', 'resident_cnt']
    )


    write_csv = PythonOperator(
        task_id='write_csv',
        python_callable=write_csv_func
    )

    start_task >> found_top >> write_csv
    #1