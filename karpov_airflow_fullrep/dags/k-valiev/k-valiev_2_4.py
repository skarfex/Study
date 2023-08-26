"""
Модуль 2, шаг 4
Дорабатываем скрипт с шага 3 модуля 2
Ходим в greenplum,
забираем из таблицы articles
значение поля heading из строки с id, равным дню недели ds
"""
from airflow import DAG
import logging

from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook  # c помощью этого hook будем входить в наш Greenplan

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'owner': 'k-valiev',
    'poke_interval': 600}

with DAG(
        dag_id='k-valiev_module2_step4',
        schedule_interval='0 3 * * 1-6',  # cron interval
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        catchup=True,
        tags=['k-valiev']
) as dag:

    start = DummyOperator(task_id='start')

    # возвращает запрос из таблицы
    def get_data_from_gp_func(exec_day):
        weekday = datetime.strptime(exec_day, '%Y-%m-%d').isoweekday()

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("get_heading_cursor")  # и именованный (необязательно) курсор

        sql = f'SELECT heading FROM articles WHERE id = {weekday}' #строка запроса
        cursor.execute(sql)  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]

        logging.info('---------------------------------------')
        logging.info(f'execution_date: {exec_day}')
        logging.info(f'day_num: {weekday}')
        logging.info(f'Article heading: {query_res}')
        logging.info('---------------------------------------')

        conn.close()


    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_data_from_gp_func,
        op_args=['{{ ds }}']
        # templates_dict={'execution_dt': '{{ start_date }}'}
    )

    end = DummyOperator(task_id='end')

    # dag
    start >> get_heading >> end
