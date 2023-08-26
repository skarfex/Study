"""
Практическое задание по уроку 4.
В рамках урока 4 создана цепочка тасков:
- extract_articles (таск подключается к БД Greenplam и возвращает из таблицы articles значение
поля heading из строки с id равному дню недели. Результат записывается в XCom)
- logging_heading_articles (таск забирает значение результата таска 'extract_articles'
из XCom и записывает его в лог)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'matveevdm',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
        dag_id='dmi-matveev_lesson4_dag',
        schedule_interval='1 3 * * 1-6',
        default_args=DEFAULT_ARGS,
        tags=['matveevdm']
        ) as dag:

    def extract_data_articles(exec_date):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor('get_str')
        cursor.execute(f'SELECT heading FROM articles where id = {int(exec_date) + 1}')
        query_res = cursor.fetchone()
        return '' if query_res is None else query_res[0]


    extract_articles = PythonOperator(task_id='extract_articles',
                                      python_callable=extract_data_articles,
                                      op_args=['{{execution_date.weekday()}}'])


    def logging_data_articles(**kwargs):
        logging.info(f"Заголовок - {kwargs['ti'].xcom_pull(task_ids='extract_articles')}")


    logging_heading_articles = PythonOperator(task_id='logging_heading_articles',
                                              python_callable=logging_data_articles,
                                              provide_context=True)

    extract_articles >> logging_heading_articles

    dag.doc_md = __doc__
    extract_articles.doc_md = """Таск подключается к БД Greenplam и возвращает из таблицы articles значение 
    поля heading из строки с id равному дню недели. Результат записывается в XCom"""
    logging_heading_articles.doc_md = """Таск забирает значение результата таска 'extract_articles'
    из XCom и записывает его в лог"""
