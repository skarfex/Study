from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import get_current_context

from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'd-sokolovskaja',
    #'poke_interval': 600
}

with DAG(
    dag_id='ds_ext_heading_dag',
    schedule_interval='0 12 * * MON,TUE,WED,THU,FRI,SAT',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['dsokolovskaia']
) as dag:

    def extract_heading_func():
        rep_dt = get_current_context()['ds']
        logging.info('Report_dt: ' + rep_dt)
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f"SELECT heading FROM articles WHERE id = extract(dow from cast('{rep_dt}' as date))")  # исполняем sql
        #query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('Heading: '+one_string)


    extract_heading = PythonOperator(
        task_id='extract_heading',
        python_callable=extract_heading_func
    )
