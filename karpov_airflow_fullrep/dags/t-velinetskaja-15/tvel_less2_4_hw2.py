"""
Обмен через XCom
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 't-velinetskaja',
    'poke_interval': 600
}

with DAG("tvel_less2_4_hw2",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['velinetskaja', 'less2_4']
         ) as dag:

    def not_sunday_func(exec_dt, **kwargs):
        exec_day = datetime.strptime(exec_dt, '%Y-%m-%d').weekday() + 2
        if exec_day == 8:
            exec_day = 1
        kwargs['ti'].xcom_push(value=str(exec_day),
                               key='wd')
        return exec_day != 7


    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=not_sunday_func,
        op_kwargs={'exec_dt': '{{ ds }}'}
    )


    def data_from_greenplum_func(**kwargs):
        wd = kwargs['ti'].xcom_pull(task_ids='not_sunday', key='wd')  # вытаскиваем сохраненный день недели

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wd}')
        res = cursor.fetchone()[0]

        kwargs['ti'].xcom_push(value=str(res), key='result')  # отправляем результат в XCOM


    data_from_greenplum = PythonOperator(
        task_id='data_from_greenplum',
        python_callable=data_from_greenplum_func
    )

    not_sunday >> data_from_greenplum