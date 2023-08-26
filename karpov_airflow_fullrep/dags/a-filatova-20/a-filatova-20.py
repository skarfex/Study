"""
Тестовый даг, который выводит сегодняшнюю дату
"""
from datetime import datetime

from airflow import DAG
import logging

# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'a-filatova-20',
    'poke_interval': 600
}

with DAG("fil_test2",
         schedule_interval='0 3 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-filatova-20']
         ) as dag:

# part1
#     dummy = DummyOperator(task_id="empty")
#
#     echo_ds = BashOperator(
#         task_id='echo_fil',
#         bash_command='echo \"Hi! Current date: \" {{ ds }}',
#         dag=dag
#     )
#
#
#     def print_curr_date():
#         print('Correct! Today is:', datetime.today().strftime('%Y-%m-%d'))
#
#
#     print_date = PythonOperator(
#         task_id='print_curr_date',
#         python_callable=print_curr_date
#     )
#
# dummy >> [echo_ds,print_date]

#part2
    def select_data(**kwargs):
        logging.info(kwargs['ds'])
        weekday = datetime.strptime(kwargs['ds'], '%Y-%m-%d').weekday() + 1
        logging.info(str(weekday))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res[0])



    select_data_from_gp = PythonOperator(
        task_id='load_csv_to_gp',
        python_callable=select_data,
        provide_context=True
    )

    select_data_from_gp

