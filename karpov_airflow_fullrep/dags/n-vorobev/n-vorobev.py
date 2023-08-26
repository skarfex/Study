from datetime import datetime

import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'n-vorobev',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(year=2022, month=3, day=1, hour=0, minute=0),
    'end_date': datetime(year=2022, month=3, day=14, hour=0, minute=0)
}

with DAG("n-vorobev",
    default_args=DEFAULT_ARGS,
    schedule_interval= '0 0 * * 1-6',
    max_active_runs=1,
    tags=['n-vorobev-1']
) as dag:

    def import_from_gp_func(**kwargs):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum') # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("cursor")  # и именованный (необязательно) курсор
        logging.info('Connection OK')

        id = datetime.today().weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {id}')  # исполняем sql
        # query_res = cursor.fetchall()  # полный результат
        logging.info('Data from DB OK')
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info('Data from DB OK')

        kwargs['ti'].xcom_push(value=one_string, key=str(id))
        logging.info("XCOM OK")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
    )

    import_from_gp = PythonOperator(
        task_id='import_from_gp',
        python_callable=import_from_gp_func,
        provide_context=True
    )

    echo_ds >> import_from_gp

