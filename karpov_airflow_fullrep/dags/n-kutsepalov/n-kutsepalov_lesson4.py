"""
Выгрузка данных из greenplum
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgress_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1), # дата начала генерации DAG Run-ов
    'end_date': datetime(2022, 3, 14),
    'owner': 'n-kutsepalov',  # владелец
    'poke_interval': 600       # задает интервал перезапуска сенсоров (каждые 600 с.)
}

# Работа через контекстный менеджер
# DAG не нужно указывать внутри каждого таска, он назначается им автоматически
with DAG("n-kutsepalov-lesson4", # название такое же, как и у файла для удобной ориентации в репозитории
    schedule_interval='0 0 * * 1-6', # расписание
    default_args=DEFAULT_ARGS,  # дефолтные переменные
    max_active_runs=1,          # позволяет держать активным только один DAG Run
    tags=['n-kutsepalov']      # тэги
) as dag:


    greenplum_extract = PythonOperator(
        task_id='greenplum_extract',
        python_callable=hello_world_func, # ссылка на функцию, выполняемую в рамках таски
        dag=dag
   )


    def log_heading_func():
      logging.info(query_res)


    log_heading = PythonOperator(
        task_id='log_heading',
        python_callable=log_heading_func, # ссылка на функцию, выполняемую в рамках таски
        dag=dag
   )

    # def load_to_greenplum_func():
    #     pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    #     conn = pg_hook.get_conn()  # берём из него соединение
    #     cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    #     cursor.execute('SELECT heading FROM articles WHERE id = 1')  # исполняем sql
    #     query_res = cursor.fetchall()  # полный результат
    #     one_string = cursor.fetchone()[0]  # если вернулось единственное значение

    load_to_greenplum = PythonOperator(
        task_id='load '
    )

    greenplum_extract >> log_heading