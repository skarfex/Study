"""
Даг, который запускается с понедельника по субботу
и забирает из таблицы articles БД GreenPlum
значение поля heading из строки с id, равным дню недели,
выводит знаение строки в логи.
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
Состоит из думми-оператора,
питон-оператора (выводит строку)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 's-bayandina',
    'poke_interval': 600
}

dag = DAG("s-bayandina_dag_4_1_new",
          schedule_interval= '0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-bayandina']
          )

dummy=DummyOperator(task_id='dummy',
                    dag=dag
                    )

def get_row_func(**kwargs):
    week_day=datetime.strptime(kwargs['date'],"%Y-%m-%d").date().weekday()+1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute('SELECT heading FROM articles WHERE id = {week_day}'.format(week_day=week_day))  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    # one_string = cursor.fetchone()  # если вернулось единственное значение
    # if one_string is not None:
    #     logging.info(one_string)
    # else:
    logging.info(query_res[0][0])


python_row = PythonOperator(task_id='python_row',
                            python_callable=get_row_func,
                            dag=dag,
                            op_kwargs={'date': '{{ ds }}'}
                            )

dummy >> python_row