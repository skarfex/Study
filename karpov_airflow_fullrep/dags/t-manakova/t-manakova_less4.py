import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.macros import datetime
from airflow.operators.python_operator import PythonOperator

from airflow import DAG

DEFAULT_ARGS = {
    'owner': 't-manakova',
    'poke_interval': 600,
    'retries': 3,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

with DAG(
    dag_id='t-manakova_less4',
    schedule_interval='0 20 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['t-manakova']
) as dag:
    
    def get_day_of_week_func(**kwargs):
        exec_day = datetime.strptime(kwargs['ds'], '%Y-%m-%d').isoweekday()
        kwargs['ti'].xcom_push(value=exec_day, key='hi')

    get_day_of_week = PythonOperator(
    task_id='get_day_of_week',
    python_callable=get_day_of_week_func,
    templates_dict={'execution_dt': '{{ ds }}'})
    
    def from_articles_to_logs_func(**kwargs):
        day_id = kwargs['ti'].xcom_pull(task_ids='get_day_of_week', key='hi')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("from_articles_to_logs_cursor")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_id}')
        query_res = cursor.fetchall()
        logging.info('_________________')
        logging.info('Дата исполнения: ' + kwargs['ds'])
        logging.info(f'День недели {day_id}')
        logging.info(query_res)
        logging.info('_________________')

    from_articles_to_logs = PythonOperator(
    task_id='from_articles_to_logs',
    python_callable=from_articles_to_logs_func)

    get_day_of_week >> from_articles_to_logs