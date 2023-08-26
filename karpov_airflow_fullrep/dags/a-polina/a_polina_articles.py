"""
Достаем название статьи из Greenplum
"""
from datetime import datetime

from airflow import DAG
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(year=2022, month=3, day=1),
    'end_date': datetime(year=2022, month=3, day=15),
    'schedule_interval': '0 10 * * MON,TUE,WED,THU,FRI,SAT',
    'owner': 'a-polina',
    'poke_interval': 600
}

with DAG("a_polina_articles",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-polina']
         ) as dag:
    def get_this_day_weekday(ds):
        return datetime.strptime(ds, '%Y-%m-%d').isoweekday()


    get_current_weekday = PythonOperator(
        task_id='get_weekday',
        python_callable=get_this_day_weekday,
        dag=dag
    )


    def get_article(**kwargs):
        ti = kwargs['ti']
        pulled_value_of_weekday = ti.xcom_pull(task_ids='get_weekday')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {pulled_value_of_weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        ti.xcom_push(value=query_res, key='article')
        return query_res


    get_article_from_greenplumdb = PythonOperator(
        task_id='get_article_from_greenplumdb',
        do_xcom_push=True,
        python_callable=get_article,
        dag=dag
    )


    def print_article_in_log(**kwargs):
        ti = kwargs['ti']
        pulled_value_of_article = ti.xcom_pull(task_ids='get_article_from_greenplumdb', key='article')
        logging.info(f'Now is {datetime.now()}')
        logging.info(pulled_value_of_article)


    print_article = PythonOperator(
        task_id='print_article',
        python_callable=print_article_in_log,
        dag=dag
    )

    get_current_weekday >> get_article_from_greenplumdb >> print_article
