"""
Даг a-chigrinova-13 по заданию
1) Проверяет, что день отработки не воскресенье
2) Подкллючается к GreenPlum и выполняет запрос по заданию
"""

from airflow import DAG
import logging
from datetime import *
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),  # начнет с 28/02/22 - нужно учесть в рассчетах
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-chigrinova-13',
    'poke_interval': 60,
}

with DAG("a-chigrinova-13_task",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-chigrinova-13']
         ) as dag:


    def is_not_sunday_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = date.weekday(datetime.strptime(execution_dt, '%Y-%m-%d')) + 2  
        # в силу отсчета дней недели с 0 и особенностей Airflow (берется предыдущий день) прибавляем 2
        return exec_day != 7

    sunday_filter = ShortCircuitOperator(
        task_id='sunday_filter',
        python_callable=is_not_sunday_func,
        templates_dict={'execution_dt': '{{ ds }}'},
        provide_context=True,
        dag=dag
        )


    def sql_command(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = date.weekday(datetime.strptime(execution_dt, '%Y-%m-%d')) + 2
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')
        query_res = cursor.fetchall()
        logging.info(query_res)

    extract_command = PythonOperator(
        task_id='extract_command',
        python_callable=sql_command,
        templates_dict={'execution_dt': '{{ ds }}'},
        provide_context=True,
        dag=dag
    )

    sunday_filter >> extract_command
