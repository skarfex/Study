import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

DEFAULT_ARGS = {
    'owner': 'p-malyshev-9',
    'poke_interval': 600,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14)
}

with DAG("p_malyshev_9_lesson4_dag",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['p-malyshev-9']
         ) as dag:
    dummy = DummyOperator(task_id="p_malyshev_9_dummy")

    def heading_from_articles_func(current_date, **kwargs):
        execution_date = datetime.strptime(current_date, "%Y-%m-%d").date().isoweekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(f'execution weekday: {execution_date}')
        cursor.execute(f'SELECT heading FROM articles WHERE id = {execution_date}')
        query_res = cursor.fetchall()
        kwargs['ti'].xcom_push(value=query_res, key='heading')
        logging.info(query_res)


    heading_from_articles = PythonOperator(
        task_id='heading_from_articles',
        op_kwargs={'current_date': "{{ ds }}"},
        python_callable=heading_from_articles_func,
        provide_context=True,
        dag=dag
    )

    dummy >> heading_from_articles
