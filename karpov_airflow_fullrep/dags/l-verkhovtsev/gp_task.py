import logging
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator, task
from airflow.utils.dates import timedelta, days_ago


DEFAULT_ARGS = {
    'owner': 'verkhovtsev',
    'wait_for_downstream': False,
    'retries': 300,
    'retry_delay': timedelta(seconds=20),
    'priority_weight': 10,
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule':  'all_success'
}

with DAG(
    dag_id="cbr_l_verkhovtsev_dag",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 3, 14, tz="UTC"),
    max_active_runs=10,
    tags=["verkhovtsev", "gp-dag"],
    schedule_interval='0 0 * * 1-6',
) as vdag:

    def load_from_greenplum(**kwargs):

        logging.info(f"kwargs: {kwargs}")
        execution_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
        logging.info(f"Execution date before strip: {execution_date}")
        execution_date = execution_date.weekday() + 1  # task numeration from 1, default numeration start from 0
        logging.info(f"Today date is {kwargs['ds']}")
        logging.info(f'Execution date: {execution_date}')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {execution_date}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат

        kwargs['ti'].xcom_push(value=query_res, key='heading')
        logging.info(f"Result: {query_res}")

        return query_res[0][0]

    gp_get_data = PythonOperator(task_id="gp_get_data",
                                 python_callable=load_from_greenplum,
                                 provide_context=True)


    gp_get_data
