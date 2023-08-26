"""
Складываем курс валют в GreenPlum
"""

from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'n-ponomarev',
    'poke_interval': 60
}

dag = DAG("np_gp",
          schedule_interval='0 0 * * MON-SAT',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['np', 'n_ponomarev']
          )


def gp_get_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")
    day_of_week = kwargs['execution_date'].weekday() + 1
    execution_date = kwargs['execution_date']
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
    query_result = cursor.fetchall()
    kwargs['ti'].xcom_push(value=query_result, key=f'heading_{day_of_week}')

    logging.info('--------------')
    logging.info(f'Actual day of week number is {day_of_week}')
    logging.info(f'Execution data is  {execution_date}')
    logging.info(f'Result {query_result}')
    logging.info('--------------')


go_to_gp = PythonOperator(
    task_id='go_to_gp',
    python_callable=gp_get_data,
    dag=dag,
    provide_context=True
)
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> go_to_gp >> end
