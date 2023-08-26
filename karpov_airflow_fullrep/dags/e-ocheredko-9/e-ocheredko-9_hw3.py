"""
Урок 3. Даг, который забирает из таблицы articles значение поля heading из строки с id, равным дню недели ds, кроме вс.
"""
from airflow import DAG
import logging
import textwrap
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'owner': 'e-ocheredko-9',
    'poke_interval': 600
}

dag = DAG(
        dag_id="e-ocheredko-9_hw3",
        schedule_interval='@daily',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['e-ocheredko-9']
)

sunday = DummyOperator(
    task_id='sunday',
    dag=dag
)

today_date = textwrap.dedent("""{{ ds }}""")


def what_day_is_it_today_(exec_date):
    if datetime.strptime(str(exec_date), '%Y-%m-%d').weekday() == 6:
        return 'sunday'
    else:
        return 'get_article_heading'


def get_article_heading_func(exec_date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute('SELECT heading FROM articles WHERE id = {id}'.format(
        id=str(datetime.strptime(str(exec_date), '%Y-%m-%d').weekday()+1)
    ))
    logging.info(str(datetime.strptime(str(exec_date), '%Y-%m-%d')))
    logging.info(str(datetime.strptime(str(exec_date), '%Y-%m-%d').weekday()+1))
    logging.info(cursor.fetchone()[0])


get_article_heading = PythonOperator(
    task_id='get_article_heading',
    python_callable=get_article_heading_func,
    op_args=[today_date],
    provide_context=True,
    dag=dag
)

what_day_is_it_today = BranchPythonOperator(
    task_id='what_day_is_it_today',
    python_callable=what_day_is_it_today_,
    op_args=[today_date],
    provide_context=True,
    dag=dag
)


what_day_is_it_today >> [get_article_heading, sunday]