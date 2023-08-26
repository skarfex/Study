"""
DAG Homework Lab 1
> Задание
1. Скачать репозиторий.
2. Настроить IDE для работы с ним.
3. Создать в папке dags папку по имени своего логина в Karpov LMS.
4. Создать в ней питоновский файл, начинающийся со своего логина.
5. Внутри создать даг из нескольких тасков:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты
6. Запушить даг в репозиторий.
7. Убедиться, что даг появился в интерфейсе airflow и отрабатывает без ошибок.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import pendulum

# import datetime
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'k-krjuchkov-21',
    # 'poke_interval': 600
}

with DAG("k-krjuchkov-21_lab-1",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-krjuchkov-21']
) as dag:

    start = DummyOperator(task_id="start")

    bash_step = BashOperator(
        task_id='bash_step',
        bash_command='echo {{ ts }}',
        trigger_rule='one_success'
    )

    def gp_select(**kwargs):
        current_date = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
        weekday = current_date.weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        # query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(f'weekday: {weekday}')
        logging.info('context ds: ' + kwargs['ds'])
        # logging.info(query_res)
        logging.info(one_string)

    python_step = PythonOperator(
        task_id='python_step',
        python_callable=gp_select,
        provide_context=True,
        trigger_rule='one_success'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    start >> [bash_step, python_step] >> end