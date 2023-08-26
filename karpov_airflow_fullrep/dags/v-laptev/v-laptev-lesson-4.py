"""
Это простейший даг.
Он состоит из сенсора (ждёт 6am),
баш-оператора (выводит execution_date),
двух питон-операторов (выводят по строке в логи)
"""

from airflow import DAG
from datetime import datetime
import logging
import pendulum

from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'owner': 'v-laptev',
    'poke_interval': 600
}

dag = DAG("v-laptev-lesson-4",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['v-laptev']
          )


def get_heading_func(load_date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f"SELECT heading from articles where id = extract(isodow from date '{load_date}')")  # исполняем sql
    #query_res = cursor.fetchall()  # полный результат
    one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(one_string)


get_heading = PythonOperator(
    task_id='get_heading',
    python_callable=get_heading_func,
    op_kwargs=dict(load_date='{{ ds }}'),
    dag=dag
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> get_heading >> end

dag.doc_md = __doc__

get_heading.doc_md = """Пишет в лог заголовки"""
