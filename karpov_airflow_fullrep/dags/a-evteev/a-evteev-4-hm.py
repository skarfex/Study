
from airflow import DAG
from airflow.utils.dates import datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-evteev',
    'poke_interval': 600
}

with DAG("a-evteev-4-hm",
          schedule_interval='0 0 * * MON-SAT',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-evteev']
          ) as dag:

    def greenplum_extract_data(weekday):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # исполняем sql
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')
        query_res = cursor.fetchone()[0]
        logging.info(f'Result: {query_res}')

    greenplum_extract_data = PythonOperator(
        task_id='greenplum_extract_data',
        op_args=['{{macros.ds_format(ds, "%Y-%m-%d", "%w")}}'],
        python_callable=greenplum_extract_data,
        dag=dag
    )

    greenplum_extract_data

