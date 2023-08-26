"""
Вывод строки из GreenPlum
"""
from airflow import DAG
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'p-ovchinnikov-20',
    'poke_interval': 600
}

with DAG("p-ovchinnikov-20-task-2",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['p-ovchinnikov-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    def greenplum_func(ds):
        dt = datetime.strptime(ds, "%Y-%m-%d")
        weekday = dt.isoweekday()
        logging.info('weekday : ' + str(weekday))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(one_string)

    greenplum_request = PythonOperator(
        task_id='greenplum_request',
        python_callable=greenplum_func,
        op_kwargs={'ds': '{{ds}}'},
        dag=dag
    )

    dummy >> greenplum_request

