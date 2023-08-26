import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from a_grishin_plugins.a_grishin_ram_operator import GetTop3LocOperator


def sqlpart(ti):
    result_location_info = ti.xcom_pull(key='return_value', task_ids='GetTop3Loc')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        query = f"""
                            CREATE TABLE IF NOT EXISTS public.a_grishin_ram_location (
                                 id int, name varchar, type varchar, dimension varchar, resident_cnt int);
                            TRUNCATE TABLE public.a_grishin_ram_location;"""
        cur.execute(query)
        for loc in result_location_info:
            query = f"""
                    INSERT INTO public.a_grishin_ram_location (id, name, type, dimension, resident_cnt)
                    VALUES ( {loc[0]}, '{loc[1]}', '{loc[2]}', '{loc[3]}', {loc[4]}
                           );
                    COMMIT;"""
            cur.execute(query)
    conn.close()

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'poke_interval': 30
}

with DAG("a-grishin_top3loc",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         tags=['a-grishin'],
         max_active_runs=1
         ) as dag:

    start = DummyOperator(task_id='start')

    GetTop3Loc = GetTop3LocOperator(
         task_id='GetTop3Loc'
    )

    sqlpartOperator = PythonOperator(
         task_id='sqlpart',
         python_callable=sqlpart
    )

    eod = DummyOperator(task_id='eod')

    start >> GetTop3Loc >> sqlpartOperator >> eod