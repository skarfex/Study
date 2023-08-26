"""

Курс DE. Раздел ETL. Урок 3, задание 1 (выполнено).
Курс DE. Раздел ETL. Урок 4, задание 1:

Даг должен:
1. Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)
2. Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
   Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS.

3. Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
4. Выводить результат работы в любом виде: в логах либо в XCom'е
5. Даты работы дага: с 1 марта 2022 года по 14 марта 2022 год

"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 3, 0, 0),
    'end_date': datetime(2022, 3, 14, 3, 0, 0),
    'owner': 'a-poluhin',
    'poke_interval': 60
}

dag = DAG("a-poluhin-lesson4",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-poluhin-first-dag']
          )

def atricle_to_logs_func(current_date):
    num_day = datetime.strptime(current_date, '%Y-%m-%d').weekday()+1
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("a_poluhin_lesson4_conn")  # и именованный (необязательно) курсор
    cursor.execute(f'SELECT heading FROM articles WHERE id = {num_day}.')  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(query_res)


def check_a_weekday(current_date):
    num_day = datetime.strptime(current_date, '%Y-%m-%d').weekday()+1
    if num_day != 7:
        return 'aricle_to_logs_operator'
    else:
        return 'sunday_operator'


aricle_to_logs_operator = PythonOperator(
    task_id='aricle_to_logs_operator',
    python_callable=atricle_to_logs_func,
    op_kwargs=dict(
            current_date='{{ ds }}'
        ),
    dag=dag
)

sunday_operator = BashOperator(
    task_id="sunday_operator",
    bash_command="echo Sunday",
    dag=dag
)

choose_a_day = BranchPythonOperator(
        task_id='check_a_weekday',
        python_callable=check_a_weekday,
        op_kwargs=dict(
            current_date='{{ ds }}'
        ),
        dag=dag
    )

choose_a_day >> [aricle_to_logs_operator, sunday_operator]
