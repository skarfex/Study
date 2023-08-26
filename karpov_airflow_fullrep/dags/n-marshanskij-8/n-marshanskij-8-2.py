from datetime import datetime
from airflow import DAG
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'marshanskij',
    'poke_interval': 20
}

with DAG('n-marshanskij-8-2',
         schedule_interval='23 23 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['marshanskij']
         ) as dag:

    def get_articles_from_gp_func(day_load):
        gp_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = gp_hook.get_conn()
        cursor = conn.cursor("s-grebenkin-cursor")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_load}')
        results = cursor.fetchall()
        logging.info("Execution day of week: " + day_load)
        logging.info("RESULT: " + str(results))


    get_articles_from_gp = PythonOperator(
        task_id='get_articles_from_gp',
        python_callable=get_articles_from_gp_func,
        op_args=['{{ logical_date.weekday() + 1 }}']
    )

    doomy = DummyOperator(task_id='start')
    doomy2 = DummyOperator(task_id='end')

    doomy >> get_articles_from_gp >> doomy2