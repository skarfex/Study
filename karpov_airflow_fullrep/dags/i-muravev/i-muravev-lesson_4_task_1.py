
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    #'end_date'  : datetime(2022,3,14),
    'owner': 'i-muravev',
    #'depends_on_past': False,
    'poke_interval': 600
}

with DAG("i-muravev-12-lesson_4",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-muravev-12-lesson_4'],
    catchup=True

     ) as dag:



   def  get_string_for_today_func(**kwargs):
       this_date = kwargs['ds'] #получаем сегодняшнюю дату  на момент запуска DAG , первая дата 01-03-2022
       print(this_date)
       day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
       print(day_of_week)
       pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук

       conn = pg_hook.get_conn()  # берём из него соединение
       print(conn)
       cursor = conn.cursor("gp_conn")  # и именованный (необязательно) курсор
       print(cursor)
       cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week};') # исполняем sql

       query_res = cursor.fetchall()
       print(query_res)
       result = query_res[0][0]
       print(result)
       return result

   get_string_for_today = PythonOperator(
       task_id='get_string_for_today',
       python_callable=get_string_for_today_func,
       provide_context=True,
       dag=dag
   )





   get_string_for_today