"""
Печатаем heading из articles в зависимости от дня недели
"""

import logging
from airflow import DAG
from datetime import datetime


from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': '2022-03-01',
    'end_date': '2022-03-14',
    'owner': 'a-galeeva-21',
    'poke_interval': 600
}

dag = DAG("a-galeeva-21_4_lesson",
          schedule_interval='0 8 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-galeeva-21']
          )


def load_csv_from_gp_func(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()  # и именованный (необязательно) курсор
    day_of_week = datetime.strptime(kwargs['date'], '%Y-%m-%d').isoweekday()
    cursor.execute(f"SELECT heading FROM articles WHERE id={day_of_week}")  # исполняем sql
    res = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.warning(res)

print_to_log = PythonOperator(
    task_id='load_csv_to_gp',
    python_callable=load_csv_from_gp_func,
    op_kwargs={
        'date': '{{ ds }}'
    },
    dag=dag
)

print_to_log
