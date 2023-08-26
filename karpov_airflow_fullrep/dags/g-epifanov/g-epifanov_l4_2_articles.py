"""
Забираем данные из таблицы articles
"""

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'g-epifanov',
    'poke_interval': 600}

with DAG(dag_id="g-epifanov_l4_2_get_articles",
          schedule_interval='0 12 * * 1-6',
          catchup=True,
          default_args=DEFAULT_ARGS,
          tags=['g-epifanov', 'hw-3']
          ) as dag:

    dummy = DummyOperator(task_id="Dummmy")

    def get_data_from_gp(**context):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_epif")
        today = context['execution_date']
        today = today.weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = {today}')
        query_res = cursor.fetchall()
        context['ti'].xcom_push(value=query_res, key='article_head')
        logging.info(f"{query_res}")


    get_articles = PythonOperator(
        task_id='get_articles',
        python_callable=get_data_from_gp,
        dag=dag)

    end = DummyOperator(task_id="Dummmy_end")

    dummy >> get_articles >> end
