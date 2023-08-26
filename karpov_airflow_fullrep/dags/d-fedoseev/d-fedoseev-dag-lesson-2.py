
"""
DAG из задания 4 урока - "Сложные пайплайны, часть 2"

> Задание
Нужно доработать даг, который вы создали на прошлом занятии.

Он должен:

+Работать с понедельника по субботу, но не по воскресеньям (можно реализовать с помощью расписания или операторов ветвления)

+Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри

+Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS либо настройте его самостоятельно в вашем личном Airflow. Параметры соединения:

Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: karpovcourses
Login: student
Password: Wrhy96_09iPcreqAS

+Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (понедельник=1, вторник=2, ...)
+Выводить результат работы в любом виде: в логах либо в XCom'е
Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года

"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-fedoseev',  # Владелец
    'poke_interval': 600  # Интервал проверки между выполнениями задач
}

with DAG("d-fedoseev-task2",
         schedule_interval='0 0 * * 1-6',
         default_args=default_args,
         max_active_runs=1,  # Максимальное кол-во запущенных параллельно дагов
         tags=['d-fedoseev'],
         ) as dag:


    def current_day_func():
        current_day = datetime.now().weekday()
        example_task_day = int(current_day) + 1
        return example_task_day

    def extract_heading_greenplum(day_of_week):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # Идентификатор подключения к PostgreSQL
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT heading FROM articles WHERE id = %s", day_of_week)  # SQL-запрос для извлечения данных
        heading = cursor.fetchall()
        cursor.close()
        connection.close()
        return heading

    def log_heading(heading):
        log.info("Heading from Greenplum: %s", heading)

    get_current_day = PythonOperator(
        task_id='get_current_day',
        python_callable=current_day_func,
        dag=dag
    )

    get_heading_from_gp = PythonOperator(
        task_id='get_heading_from_gp',
        python_callable=extract_heading_greenplum,
        op_args=[get_current_day.output],
        dag=dag
    )

    log_results = PythonOperator(
        task_id='log_results',
        python_callable=log_heading,
        op_args=[get_heading_from_gp.output],
        dag=dag
    )

    get_current_day >> get_heading_from_gp >> log_results