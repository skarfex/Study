"""
lesson 4
"""
from airflow import DAG
import datetime, logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'il-pastuhov',
    'start_date':  datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
    'poke_interval': 1
}
#new version 2

def get_weekday(ds):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    logging.info('GreenPlum connect')
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor()
    week_day = datetime.datetime.strptime(ds, '%Y-%m-%d').isoweekday()
    sql = f'SELECT heading FROM articles WHERE id = {week_day}'
    cursor.execute(sql)  # исполняем sql
    res = cursor.fetchall()[0]  # если вернулось единственное значение
    logging.info(f'weekday: {week_day}')
    logging.info(f'heading: {res}')
    cursor.close()
    conn.close()

with DAG('il-pastuhov_ds_test_lesson_4',
         schedule_interval='0 10 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['il-pastuhov']) as dag:


    dummy = DummyOperator(task_id="dummy")

    py_weekday = PythonOperator(
        task_id='py_weekday',
        python_callable=get_weekday,
        op_kwargs = {'ds': '{{ ds }}'},
        dag=dag
    )

    dummy >> py_weekday