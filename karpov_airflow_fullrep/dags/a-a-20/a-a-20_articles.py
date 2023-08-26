
"""
Задача дага: 
    Работать c понедельника по субботу, но не по воскресеньям;
    Забирать из таблицы articles значение поля heading из строки c id, равным дню недели ds;
    Выводить результат работы в логах;
    Даты работы дага: c 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
import logging
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'owner': 'a-a-20'
    , 'start_date': datetime.datetime(2022, 3, 1)
    , 'end_date': datetime.datetime(2022, 3, 14)
    , 'poke_interval': 1
}


def PythonOperator_func(ds):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()  # курсор
    int_week_day = datetime.datetime.strptime(ds, '%Y-%m-%d').isoweekday()
    cursor.execute(f'SELECT heading FROM articles WHERE id = {int_week_day}')  # исполняем sql
    query_res = cursor.fetchall()[0]  # первое значение из курсора
    logging.info(f'First heading = {query_res}') # пишем в лог
    cursor.close() # закрываем курсо 
    conn.close() # закрываем коннект 
    


with DAG('a-a-20_articles',
    schedule_interval='0 10 * * 1-6', # Работать c понедельника по субботу, но не по воскресеньям; c 10 am Msk, c 7 am UTC
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-a-20']
) as dag:

    dummy = DummyOperator(task_id="dummy")


    PythonOperator_echo_heading = PythonOperator(
        task_id='PythonOperator_echo_heading',
        python_callable=PythonOperator_func,
        op_kwargs = {'ds': '{{ ds }}'},
        dag=dag
    )

    dummy >> PythonOperator_echo_heading
