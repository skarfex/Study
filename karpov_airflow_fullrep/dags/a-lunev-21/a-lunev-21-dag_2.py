import logging
import datetime
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'a-lunev-21',
    'start_date': datetime.datetime(2022, 3, 1),
    'end_date': datetime.datetime(2022, 3, 14),
}

def get_data_func(ds):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    wday = datetime.datetime.strptime(ds, '%Y-%m-%d').isoweekday()

    cursor.execute(f'SELECT heading FROM articles WHERE id = {wday}')
    result = cursor.fetchall()
    
    logging.info(f'row: {result}')

    cursor.close()
    connection.close()


with DAG(
    dag_id='a-lunev-21-dag_2',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-lunev']
) as dag:

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data_func,
    )

    get_data
