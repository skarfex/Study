"""
Даг для урока 4 (данные из GreenPlum).
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import timedelta, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'm-stratonnikov',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG("m-stratonnikov-less4",
          schedule_interval='0 0 * * 1-6',#запуск с понедельника по субботу
          default_args=default_args,
          max_active_runs=1,
          tags=['m-stratonnikov']
          ) as dag:

    begin = DummyOperator(task_id='begin')

    def takin_from_db_func(**context):#функция извлечения данны из БД
        data_interval_start = context['data_interval_start']
        weekday = data_interval_start.weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')# инициализируем хук
        conn = pg_hook.get_conn() # берём из него соединение
        cur = conn.cursor() #создаём курсор
        qry = str(f'SELECT heading FROM articles WHERE id = {weekday}') #формируем запрос
        cur.execute(qry) #выполняем запрос
        rez = cur.fetchall()
        conn.close()
        logging.info(str(weekday) + " weekday execute")
        context['ti'].xcom_push(value=rez, key='article')

    takin_from_db = PythonOperator(
        task_id='takin_from_db',
        python_callable=takin_from_db_func,
        provide_context=True,
        dag = dag
        )

    end = DummyOperator(task_id='end')

    begin >> takin_from_db >> end
