"""
Даг для Урока 4
    Получение heading из таблицы articles за день недели запуска ДАГа
"""

from airflow import DAG
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': ' va-borisov'
}

with DAG("va-borisov_lesson_4",
        schedule_interval='0 0 * * MON-SAT',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['lesson_4']
) as dag:
    
    bash_ds = BashOperator(task_id='bash_ds',
                           bash_command='echo {{ ds }}')

    def get_heading_articles(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id=kwargs['conn'])  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор

        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        
        cursor.execute('SELECT heading FROM articles WHERE id = {weekday}'.format(weekday=weekday))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        
        for row in query_res:
            logging.info(row)
        
        cursor.close()
        conn.close()
    
    get_heading_articles_task = PythonOperator(task_id='get_heading_articles_task',
                                               python_callable=get_heading_articles,
                                               op_kwargs={
                                                   'conn': 'conn_greenplum',
                                                   'ds': '{{ ds }}'
                                               })
    
    bash_ds >> get_heading_articles_task

    
    
