"""
Д/З по 3 и 4 урокам модуля Airflow.
"""

from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),  # начинается с полночи 1 марта 2022
    'end_date': datetime(2022, 3, 14, hour=1),  # окончание с небольшим запасом на случай сбоев и перезапусков
    'owner': 'a-chumikov',
    'poke_interval': 500
}

with DAG("dag_for_lessons_4_and_5",
         schedule_interval='0 0 * * 1-6',  # запуск в полночь каждого дня кроме воскресенья
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-chumikov']
         ) as my_dag:

    dummy = DummyOperator(task_id='dummy')  # 1-я часть д/з

    echo_ds = BashOperator(  # 1-я часть д/з
        task_id='echo_ds',
        bash_command='echo {{ ds }}')


    def get_day_of_week(**kwargs):  # 2-я часть д/з
        """Функция для получения дня недели из даты запуска дага и передача значения в xcom.
        Так как, фактически, дата запуска сдвигается на предыдущий день из-за UTC, то, чтобы
        её вернуть на текущую, использую tomorrow_ds"""

        parse_ds = map(int, kwargs['tomorrow_ds'].split('-'))
        day_of_week = datetime(*parse_ds).weekday() + 1  # т.к. нумерация weekday() идёт с нуля
        kwargs['ti'].xcom_push(value=day_of_week, key='weekday_or_id')  # явный пуш выходных данных


    python_task = PythonOperator(
        task_id='python_task',
        python_callable=get_day_of_week,
        provide_context=True)


    def greenplum_action(**kwargs):  # 2-я часть д/з
        """Основная функция для получения данных из Greenplum с
        помощью дня недели, взятого из xcom таска python_task"""

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # создаём курсор

        pulled_xcom = kwargs['ti'].xcom_pull(task_ids='python_task', key='weekday_or_id')
        cursor.execute("SELECT heading FROM public.articles WHERE id = %s", (pulled_xcom,))  # исполняем sql
        logging.info(cursor.fetchall())  # записываем получение данных в логи
        # теоретически может быть более 1-й строки в выдаче (т.к. первичный ключ в таблице не задан), поэтому fetchall()


    greenplum_task = PythonOperator(
        task_id='greenplum_task',
        python_callable=greenplum_action)


    dummy >> [echo_ds, python_task] >> greenplum_task


my_dag.doc_md = __doc__

