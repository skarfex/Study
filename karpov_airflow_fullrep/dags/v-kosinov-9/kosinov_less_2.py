"""
less_2 22
"""
from airflow import DAG
from airflow.utils.dates import datetime
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v-kosinov',
    'depends_on_past': False
}

with DAG("kosinov-less-2",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-kosinov']
    ) as dag:


    def is_weekday_func(execution_dt, **kwargs):
        logging.info(str(execution_dt))
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        kwargs['ti'].xcom_push(value=exec_day, key='day')
        return exec_day in [1,2,3,4,5,6]


    weekday_only = ShortCircuitOperator(
        task_id='weekday_only',
        python_callable=is_weekday_func,
        provide_context=True,
        op_kwargs={'execution_dt': '{{ds}}'}
        )

    def green_con(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        day = kwargs['ti'].xcom_pull(task_ids='weekday_only', key='day')
        logging.info(str(day))
        req = 'SELECT heading FROM articles WHERE id =' + str(day)
        cursor.execute(req)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(query_res)

    green_info = PythonOperator(
        task_id='green_info',
        python_callable=green_con,
        provide_context=True
    )

    weekday_only >> green_info