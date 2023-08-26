# Задание 3
#

from datetime import datetime
import logging
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'o-makarova-20',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'email': ['obmakarova@beeline.ru'],
    'poke_interval': 300,
    'retries': 2
    }

with DAG("o-makarova-20_first_dag",
    schedule_interval='05 * * 1,2,3,4,5,6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-makarova-20']
    ) as dag:

#    dummy = DummyOperator(task_id="dummy")

#    echo_ds = BashOperator(
#        task_id='echo_ds',
#        bash_command='echo {{ ds }}',
#        dag=dag
#   )


    def ex_date(**kwargs):
        logging.info(kwargs['ds'])
        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        logging.info(str(weekday))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {int(weekday)+1}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res[0])  # если вернулось единственное значение


    ex_data = PythonOperator(
        task_id ='ex_data',
        python_callable = ex_date,
        provide_context = True
    )

ex_data
