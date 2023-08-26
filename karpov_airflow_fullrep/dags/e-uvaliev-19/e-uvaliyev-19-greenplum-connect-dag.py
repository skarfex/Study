"""
Даг по Уроку 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from textwrap import dedent
import pendulum
import logging


from airflow.operators.bash import BashOperator
#from airflow.operators.python.pythonoperator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2023, 4, 19, tz="UTC"),
    "end_date": pendulum.datetime(2023, 4, 26, tz="UTC"),
    'owner': 'e-uvaliev-19',
    'poke_interval': 600
    
}

with DAG("e-uvaliev-19-greenplum-connect-dag",
    schedule_interval="0 0 * * 1-6",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-uvaliev-19']
    ) as dag:


    def get_articles(execution_date):
        day_id = execution_date.strftime('%w')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {int(day_id)}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(query_res[0][0])


get_article_heading = PythonOperator(
    task_id='get_article_heading',
    python_callable=get_articles,
    provide_context=True,
    dag=dag,
)