"""
Даг 2 дз
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.dates import days_ago
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    'owner': 'a.danilov-16',
    # 'start_date': days_ago(2),
    'poke_interval': 600,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
}


with DAG("a.danilov-16_dz",
         schedule_interval='@weekly',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a.danilov-16']
         ) as dag:

    def hello_world():
        logging.info('Hello world')

    start = DummyOperator(task_id='start')

    # start_bash = BashOperator(
    #     task_id='start_bash',
    #     bash_command='echo "dag start"'
    # )

    def is_weekend_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day != 6

    not_weekend_only = ShortCircuitOperator(
        task_id='not_weekend_only',
        python_callable=is_weekend_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    def python_task(**kwargs):
        today = kwargs['templates_dict']['today']
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор

        logging.info(today)

        sql = """
            SELECT heading FROM articles WHERE id = {value}
        """.format(value=datetime.strptime(today, '%Y-%m-%d').weekday() + 1)

        logging.info(sql)
        cursor.execute(sql)  # исполняем sql

        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res)
        # one_string = cursor.fetchone()[0]

    task1 = PythonOperator(
        task_id='task1',
        python_callable=python_task,
        templates_dict={'today': '{{ ds }}'}
    )

    end_bash = BashOperator(
        task_id='end_bash',
        bash_command='echo "dag finish"'
    )

    start >> not_weekend_only >> task1 >> end_bash
