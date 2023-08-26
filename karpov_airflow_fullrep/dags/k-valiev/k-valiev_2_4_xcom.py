"""
Модуль 2, шаг 4
Дорабатываем скрипт с шага 3 модуля 2
Ходим в greenplum,
забираем из таблицы articles
значение поля heading из строки с id, равным дню недели ds
вариант с xcomm
"""
from airflow import DAG
import logging

from datetime import datetime

from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook  # c помощью этого hook будем входить в наш Greenplan

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    # 'end_date': datetime(2022, 3, 15),
    'owner': 'k-valiev',
    'poke_interval': 600}

with DAG(
    dag_id='k-valiev_module2_step4_xcom',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=True,
    tags=['k-valiev']
    ) as dag:

    start = DummyOperator(task_id='start')

    def get_week_day_func(**kwargs):
        ds=datetime.strptime(kwargs['ds'], '%Y-%m-%d') #дата на момент запуска DAG
        weekday=ds.isoweekday() #извлекаем порядковый номер дня недели
        # передаем в xcomm значение переменной weekday и ds (дата старта DAG)
        kwargs['ti'].xcom_push(value=ds, key='ds')
        kwargs['ti'].xcom_push(value=weekday, key='week_day')
        logging.info(f'ds: {ds}')
        logging.info(f'weekday: {weekday}')

    get_week_day = PythonOperator(
        task_id='get_week_day',
        python_callable=get_week_day_func,
        provide_context=True
    )

    # возвращает запрос из таблицы
    def get_data_from_gp_func(**kwargs):
        #забираем номер дня недели из таска 'get_week_day' по ключу 'week_day' и ds
        ds = kwargs['ti'].xcom_pull(task_ids='get_week_day', key='ds')
        weekday = kwargs['ti'].xcom_pull(task_ids='get_week_day', key='week_day')

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("get_heading_cursor")  # и именованный (необязательно) курсор

        sql = f'SELECT heading FROM articles WHERE id = {weekday}' #строка запроса
        cursor.execute(sql)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]

        logging.info('---------------------------------------')
        logging.info(f'execution_date: {ds}')
        logging.info(f'day_num: {weekday}')
        logging.info(f'Article heading: {query_res}')
        logging.info('---------------------------------------')

        conn.close()


    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_data_from_gp_func,
        provide_context=True
        # op_args=['{{ ds }}']
        # templates_dict={'execution_dt': '{{ start_date }}'}
    )

    end = DummyOperator(task_id='end')

    # dag
    start >> get_week_day >>get_heading >> end
