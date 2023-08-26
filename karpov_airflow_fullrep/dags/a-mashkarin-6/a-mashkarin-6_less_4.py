from airflow import DAG
import logging
#from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator

from datetime import datetime  

from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-mashkarin-6'
}

dag = DAG('a-mashkarin-6_less_4_fix',
          schedule_interval='0 3 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-mashkarin-6'])


def from_gp(**kwargs):
    day_of_week = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday()+1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')  # исполняем sql
    logging.info(cursor.fetchall())  # полный результат


print_result = PythonOperator(
    task_id='print_result',
    python_callable=from_gp,
    provide_context=True,
    dag=dag,
)

print_result
