"""
Урок 4 часть 2. DAG для выборки данных из GP
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-panteleeva',
    'poke_interval': 600
}
GREENPLUM_CONN = 'conn_greenplum'

with DAG("a-panteleeva_les4_p2",
         schedule_interval='0 3 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-panteleeva_l4_p2']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    def print_row_func():
        dt = datetime.today()
        logging.info(dt.strftime('%d-%m-%Y'))
        dow = dt.isoweekday()

        pg_hook = PostgresHook(postgres_conn_id=GREENPLUM_CONN)  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = ' + str(dow))  # исполняем sql
        # query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(one_string)


    print_row = PythonOperator(
        task_id='print_row',
        python_callable=print_row_func,
        dag=dag
    )

    dummy >> [echo_ds, print_row]
