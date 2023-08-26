import logging
from datetime import date, datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task


# Функции для реализации Taskflow API
@task
def return_weekday_func():
    w_day = date.today().weekday() + 1
    logging.info(f"Порядковый номер текущего дня: {w_day} \n,День: {date.today()}")
    return w_day


@task
def get_row_in_db(num):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute('SELECT heading FROM articles WHERE id = %s', (num,))
        query_res = cur.fetchall()[0][0]
        logging.info(f"heading in table articles: {query_res}")


# Функции для реализации через оператор with
def return_execution_dt(**kwargs):
    val = datetime.strptime(kwargs['templates_dict']['execution_dt'], '%Y-%m-%d').weekday() + 1
    logging.info(f"Порядковый номер текущего дня: {val}")
    kwargs['ti'].xcom_push(value=val, key='key_ds')


def pull_ds_weekday_num(**kwargs):
    num = kwargs['ti'].xcom_pull(task_ids='return_ds', key='key_ds')
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute('SELECT heading FROM articles WHERE id = %s', (num,))
        query_res = cur.fetchall()[0][0]
        logging.info(f"heading in table articles: {query_res}")


def logging_done_update(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
    conn = pg_hook.get_conn()
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM a_ryzhkov_18_ram_location')
        query_res = cur.fetchall()[0][0]
        logging.info(f"Операция прошла {'Успешно!!'if query_res > 0 else 'С ошибкой'}")