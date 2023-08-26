"""
Show exchange rate in airflow console
"""
from airflow import DAG
import datetime as dt
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022, 2, 28),
    'end_date': dt.datetime(2022, 3, 14),
    'owner': 'v-galashov',
    'poke_interval': 100
}

dag = DAG("v-galashon_lesson_4_update_cbr_script",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-galashov']
          )

def export_heading_articles_func(exec_dt,**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = extract(isodow from date {exec_dt} )')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info("-----------")
    logging.info(query_res)
    logging.info(one_string)
    logging.info("-----------")

export_heading_articles = PythonOperator(
    task_id='export_heading_articles',
    python_callable=export_heading_articles_func,
    op_args=['{{ ds }}'],
    dag=dag
)

export_heading_articles
