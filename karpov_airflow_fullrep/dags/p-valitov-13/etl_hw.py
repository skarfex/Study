"""
HW lesson 4
"""


from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import pendulum
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.weekday import DayOfWeekSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
        'start_date': datetime(2022, 3, 1),
        'end_date': datetime(2022, 3, 14),
        'owner': 'p-valitov-13',
        'poke_interval': 600
    }

with DAG("pvalitov_hw_les_4",
        schedule_interval='0 0 * * 2-7',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-valitov-13']
    ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def get_article_by_id(article_id):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("gp_conn")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {article_id};')
        return cursor.fetchone()[0]


    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_by_id,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ],
        do_xcom_push=True,
        dag=dag
    )

    dummy >> get_article
