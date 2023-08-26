"""
Getting articles headings from GreenPlum
and printing them based on the execution day number
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-korobova',
    'poke_interval': 120
}


def show_execution_day_func(ds, **kwargs):
    """
    Getting the string that contains the execution day and the day number
    """
    exec_date = ds
    day_number = datetime.strptime(ds, '%Y-%m-%d').isoweekday()
    current_day_result = f'Execution date: {exec_date}, day number: {day_number}'
    logging.info(current_day_result)


def get_article_heading_func(ds, **kwargs):
    """
    Getting the article headings that are matched to the execution day number
    """

    sql_request = 'SELECT heading FROM articles WHERE id = {gen_id}'.format(
        gen_id=datetime.strptime(ds, '%Y-%m-%d').isoweekday()  # generate id based on the execution day
    )

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("named_cursor_name")
    cursor.execute(sql_request)
    query_res = cursor.fetchall()
    for i in query_res:
        logging.info('Article heading: {title}'.format(title=i[0]))

with DAG(
        "a-korobova-articles",
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['a-korobova']
) as dag:
    show_execution_day = PythonOperator(
        task_id='show_execution_day',
        python_callable=show_execution_day_func,
        dag=dag
    )

    get_article_heading = PythonOperator(
        task_id='get_article_heading',
        python_callable=get_article_heading_func,
        dag=dag
    )

    show_execution_day >> get_article_heading
