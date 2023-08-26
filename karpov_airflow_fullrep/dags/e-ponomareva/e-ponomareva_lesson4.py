"""
Урок 4. Забирать из таблицы GP articles значение поля heading из строки с id, равным дню недели ds
"""

from airflow import DAG
from datetime import datetime
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),  # Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-ponomareva',
    'poke_interval': 600
}

with DAG("e-ponomareva_lesson4",
         schedule_interval='0 0 * * MON-SAT',  #Работать с понедельника по субботу, но не по воскресеньям
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['e-ponomareva']
         ) as dag:

    def load_from_gp_func(exec_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')   # инициализируем хук
        conn = pg_hook.get_conn()                                   # берём из него соединение
        cursor = conn.cursor("named_cursor_name")                   # и именованный курсор
        cursor.execute(f"SELECT heading FROM public.articles WHERE id = extract(isodow from date '{exec_date}')")
        query_res = cursor.fetchall()                               # полный результат
        logging.info(query_res)

    load_from_gp = PythonOperator(
        task_id='load_from_gp',
        python_callable=load_from_gp_func,
        op_kwargs=dict(exec_date = '{{ ds }}'),
        dag = dag
    )

    load_from_gp