"""
Нужно доработать даг, который вы создали на прошлом занятии.
Он должен:
1. Работать с понедельника по субботу, но не по воскресеньям
2. Ходить в наш GreenPlum. Вариант решения — PythonOperator с PostgresHook внутри
Используйте соединение 'conn_greenplum' в случае, если вы работаете из LMS
либо настройте его самостоятельно в вашем личном Airflow.
Параметры соединения:
Host: greenplum.lab.karpov.courses
Port: 6432
DataBase: karpovcourses
Login: student
Password: Wrhy96_09iPcreqAS
3. Забирать из таблицы articles значение поля heading из строки с id, равным дню недели ds (пон=1, вторник=2, ...)
4. Выводить результат работы в любом виде: в логах либо в XCom'е
5. Даты работы дага: с 1 марта 2022 года по 14 марта 2022 года
"""

from airflow import DAG
# from airflow.sensors.weekday import DayOfWeekSensor
from datetime import datetime
import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator


DEFAULT_ARGS = {'start_date': datetime(2022, 3, 1),
                'end_date': datetime(2022, 3, 14),
                'owner': 'n-chepurnyh-19',
                'poke_interval': 600}
with DAG("n-chepurnyh-19_etl_4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['n-chepurnyh-19']
         ) as dag:

    start = DummyOperator(task_id='start')


    def is_weekend_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return exec_day in [0, 1, 2, 3, 4, 5]
    no_sunday = ShortCircuitOperator(
        task_id='no_sunday',
        python_callable=is_weekend_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    def greenplum_func(execution_dt):
        day_of_week = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        logging.info(f'day of week = {day_of_week}')
        cursor = conn.cursor("named_cursor_name")
        query_in = 'SELECT heading FROM articles WHERE id = ' + str(day_of_week+1)
        cursor.execute(query_in)
        # query_res = cursor.fetchall()
        one_string = cursor.fetchone()[0]
        logging.info(one_string)

    python_op = PythonOperator(task_id='python_op',
                               python_callable=greenplum_func,
                               op_kwargs={'execution_dt': '{{ ds }}'},
                               dag=dag)

start >> no_sunday >> python_op

