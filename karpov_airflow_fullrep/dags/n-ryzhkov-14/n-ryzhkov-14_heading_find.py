"""
Dag забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds
"""

from airflow import DAG
import logging
import datetime

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'owner': 'n-ryzhkov-14',
    'poke_interval': 600
}


with DAG("n-ryzhkov-14_find_in_greenplan",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['n-ryzhkov-14']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def greenplan_find_func(week_day, **kwargs):
        pg_hook = PostgresHook('conn_greenplum')
        id = datetime.strptime(week_day, '%Y-%m-%d').isoweekday()
        logging.info(f"Get text of the article {id}")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id}')
        query_res = cursor.fetchone()[0]
        conn.close()
        logging.info(f"Get text of the article '{query_res}'")
        kwargs['ti'].xcom_push(value=query_res, key='heading')


    greenplum_task = PythonOperator(
        task_id='greenplum_task',
        op_kwargs={'week_day': "{{ ds }}"},
        python_callable=greenplan_find_func,
        dag=dag
    )

    dummy >> greenplum_task
