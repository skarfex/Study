"""
Этот DAG выполняет задание 4 урока
"""

from airflow import DAG
import datetime,time,logging,pendulum
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'owner': 'a-krysanov-18',
    'start_date': pendulum.datetime(2022, 3, 1),
    'end_date': pendulum.datetime(2022, 3, 15),
    'retries': 4,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG('a-krysanov-18-lesson4-ver5',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-krysanov-18']) as dag:

    def get_weekday():
        context = get_current_context()
        pre_ds_txt = context['ds']
        pre_ds = datetime.datetime.strptime(pre_ds_txt, "%Y-%m-%d")
        week_day = datetime.datetime.isoweekday(pre_ds)
        sql = f'SELECT heading FROM articles WHERE id = {week_day}'
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()
        cursor.execute(sql)  # исполняем sql
        #query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(f'weekday: {week_day}')
        logging.info(f'heading: {one_string}')
    
    py_weekday = PythonOperator(
        task_id='weekday',
        python_callable=get_weekday
    )
    
    py_weekday
