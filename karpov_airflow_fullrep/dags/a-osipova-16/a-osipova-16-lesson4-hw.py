"""
HW. Lesson 4. More complex pipeline. Part 2.
Getting the article headings from DB.
"""

from airflow import DAG
from datetime import timedelta, datetime
import logging
import pendulum


from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'a-osipova-16',
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'poke_interval': 600
}

with DAG("a-osipova-16-lesson4-hw",
         schedule_interval='0 0 * * 1-6', #в полночь все дни кроме воскресенья
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-osipova-16']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')


    def get_heading_from_db(ds, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        week_day_num = datetime.strptime(ds, '%Y-%m-%d').isoweekday() # номер дня недели начиная с понедельника
        logging.info(f"Week_day_num = {week_day_num}")
        logging.info(f"Get the heading of article № {week_day_num}")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {week_day_num}') #ID статьи = номер дня недели
        query_res = cursor.fetchone()[0]  # результат будет один
        #query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        conn.close()
        logging.info(f"The heading '{query_res}'")
        kwargs['ti'].xcom_push(value=query_res, key='heading')


    get_heading = PythonOperator(
        task_id='get_heading_from_db',
        op_kwargs={'ds': "{{ ds }}"}, #The DAG run’s logical date as YYYY-MM-DD
        python_callable=get_heading_from_db,
        provide_context=True
    )

    dummy >> get_heading


dag.doc_md = __doc__