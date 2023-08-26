"""
Передает через XCom значение поля heading из строки с id, равным дню недели ds, из таблицы articles karpovcourses greenplum database
Второй таск получает эту строку и пишет её в логи
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import datetime as dt

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': dt.datetime(2022,12,9),
    'owner': 'g-moshnina',
    'poke_interval': 600
}

with DAG("moshnina_get_heading_xcom_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['g-moshnina']
) as dag:

    def get_heading_from_gp():
        weekday = dt.datetime.today().weekday()
        logging.info(f'Today is a {weekday} day of the week')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}') 
        one_string = cursor.fetchone()[0] 
        conn.close()
        
        logging.info('Heading is successfully read')

        return one_string

    heading_from_gp = PythonOperator(
        task_id='heading_from_gp',
        python_callable=get_heading_from_gp
    )

    def XCom_to_log(**kwargs):
        logging.info('______________________________________________________________')
        logging.info('This is a heading from karpovcourses greenplum with id equal day of the week')
        logging.info(kwargs['templates_dict']['heading'])
        logging.info('______________________________________________________________')

    XCom_to_log = PythonOperator(
        task_id='XCom_message_to_log',
        python_callable=XCom_to_log,
        templates_dict={'heading': '{{ ti.xcom_pull(task_ids="heading_from_gp") }}'},
        provide_context=True
    )

    heading_from_gp >> XCom_to_log
