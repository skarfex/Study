#Тестовый даг
#d-drovosekov-dag1_task2
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,15),
    'owner': 'd-drovosekov',
    'poke_interval': 600
}

with DAG("d-drovosekov-dag1_task2",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-drovosekov']
        ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def current_date_func(ds):
        logging.info(ds)


    def data_from_gp_func(ds):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT heading FROM articles WHERE id in ({0})'.format(datetime.strptime(ds,'%Y-%m-%d').date().isoweekday()))
        logging.info(cursor.fetchall())



    data_from_gp = PythonOperator(
        task_id='data_from_gp',
        python_callable=data_from_gp_func,
        dag=dag
    )

    current_date = PythonOperator(
        task_id='current_date',
        python_callable=current_date_func,
        dag=dag
    )

    dummy >> [echo_ds, current_date,data_from_gp]