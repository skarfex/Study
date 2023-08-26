
"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'i-katser',
    'poke_interval': 600
}

with DAG("DAG_i-katser_2",
    start_date = datetime(2022, 3, 1),
    end_date = datetime(2022, 3, 14),
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['i-katser'],
    catchup=True
) as dag:

    dummy = DummyOperator(task_id="dummy")


    def conn_bd(**context):
        wek = context['execution_date'].isoweekday()
        pg_hook = PostgresHook('conn_greenplum')
        db_data = pg_hook.get_conn()
        cur = db_data.cursor()
        logging.info(f'weekday: {wek}')
        cur.execute(f'''SELECT heading FROM articles WHERE id = {wek};''')
        r = cur.fetchall()

        logging.info(f'data: {r}')


    conn_bd = PythonOperator(
        task_id='conn_bd',
        python_callable=conn_bd,
        dag=dag
    )

    dummy >> conn_bd


