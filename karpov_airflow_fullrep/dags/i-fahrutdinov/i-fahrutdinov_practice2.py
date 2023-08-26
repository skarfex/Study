"""
Даг для практики 2
"""
from airflow import DAG
import logging
import pendulum

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'i-fahrutdinov',
    'poke_interval': 600,
}

with DAG("i-fahrutdinov_practice2",
         schedule_interval='0 0 * * MON-SAT',
         start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
         end_date=pendulum.datetime(2022, 3, 14, tz='UTC'),
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['i-fahrutdinov'],
         catchup=True,
         ) as dag:

    dummy = DummyOperator(task_id="start")

    def going_gp(**context):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        day_num = context['logical_date'].weekday()+1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_num}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(f"res = {query_res}")  # в лог

    gp_results = PythonOperator(
        task_id='going_gp',
        python_callable=going_gp,
        dag=dag
    )
    dummy >> gp_results
