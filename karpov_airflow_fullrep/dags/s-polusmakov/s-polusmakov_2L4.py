"""4 урок. Сложные пайпланы. Часть 2."""

from datetime import date, datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 's-polusmakov'
}

with DAG("psv_2L4",
         schedule_interval='0 0 * * MON-SAT',
         default_args=DEFAULT_ARGS,
         tags=['s-polusmakov']
         ) as dag:

    def q_to_gp(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        dw_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {dw_date}')  # исполняем sql
        query_res = cursor.fetchone()[0]  # полный результат
        logging.info(f'Log 2L4: {query_res}')


    result_query_to_log = PythonOperator(
        task_id='result_query_to_log',
        python_callable=q_to_gp,
        provide_context=True
    )

    start = DummyOperator(task_id='start')

    start >> result_query_to_log





