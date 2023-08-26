"""
Задание 1
"""
from airflow import DAG
import logging
import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from eg_lebedev_plugins.eg_lebedev_ram_operator import EgLebedevRamOperator

DEFAULT_ARGS = {
    'owner': 'eg-lebedev',
    'poke_interval': 60,
    'start_date': datetime.datetime(2022, 10, 3)
}

tableName = "eg_lebedev_ram_location"

def createTable(**kwargs):
    hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = hook.get_conn()

    with conn.cursor() as cursor:
        q = "create table if not exists {} (id serial PRIMARY KEY, name varchar, type varchar, dimension varchar, resident_cnt integer);".format(tableName)
        logging.info(q)
        cursor.execute(q)
        conn.commit()
    with conn.cursor("sd") as cursor:
        q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}';".format(tableName)
        logging.info(q)
        cursor.execute(q)
        res = cursor.fetchall()
        logging.info(res)
    logging.info("table created")

def checkC():
    hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = hook.get_conn()    
    with conn.cursor('some_cur') as cursor:
        q = "select * from {};".format(tableName)
        logging.info(q)
        cursor.execute(q)
        logging.info(cursor.fetchall())


with DAG("ds_l5_v2",
    schedule_interval='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['eg-lebedev']
) as dag:
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=createTable
    )

    extract = EgLebedevRamOperator(
        task_id='extract_ram',
    )

    check = PythonOperator(
        task_id='check_result',
        python_callable=checkC
    )

    create_table >> extract >> check