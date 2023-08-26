"""
4 урок Забирать значения из таблицы articles
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 4),# дата начала генерации DAG Run-ов
    'end_date': datetime(2022, 3, 14, 4),
    'owner': 'n-novikova-16',  # владелец
    'poke_interval': 600       # задает интервал перезапуска сенсоров (каждые 600 с.)
}

# Работа через контекстный менеджер
# DAG не нужно указывать внутри каждого таска, он назначается им автоматически
with DAG("n-novikova-lesson-4", # название такое же, как и у файла для удобной ориентации в репозитории
    schedule_interval='0 0 * * 1-6', # интервал запуска
    default_args=DEFAULT_ARGS,  # дефолтные переменные
    max_active_runs=1,          # позволяет держать активным только один DAG Run
    tags=['n-novikova-16']      # тэги
) as dag:

    def get_article(week_day, **kwargs):
        pg_hook = PostgresHook('conn_greenplum') #подключаемся к гринпламу
        id = datetime.strptime(week_day, '%Y-%m-%d').isoweekday()
        logging.info(f"День недели: {id}")
        query_res = 'Ничего'
        if id < 7:
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(f'SELECT heading FROM articles WHERE id = {id}')
            query_res = cursor.fetchone()[0] # возвращаем единственное значение
            conn.close()
        else:
            logging.info('Сегодня воскресенье')

        logging.info(f"Результат: '{query_res}'")
        kwargs['ti'].xcom_push(value=query_res, key='heading') # результат - ключ, значение

    select_text = PythonOperator(
        task_id='select_text',
        op_kwargs={'week_day': "{{ ds }}"},
        python_callable=get_article,
        provide_context=True,
        dag = dag
    )

    end_task = DummyOperator(task_id='end_task')

    select_text >> end_task