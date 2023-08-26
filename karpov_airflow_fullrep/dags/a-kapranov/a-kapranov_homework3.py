from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, date
import logging


DEFAULT_ARGS = {
    'owner': 'a-kapranov',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'poke_interval': 600
}


with DAG("a-kapranov_homework3",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-kapranov'],
         catchup=True) as dag_kapranov:

    # Добавляем оператор DummyOperator
    start_task = DummyOperator(task_id='start_task')

    # date_task = BashOperator(
    #     task_id='date_task',
    #     bash_command='echo {{ ds }}')
    #
    # def print_date():
    #     print(datetime.now())
    #
    # # Добавляем оператор PythonOperator с выводом даты
    # python_task = PythonOperator(
    #     task_id='python_task',
    #     python_callable=print_date
    # )

    def get_art(_day):
        today_weekday = date.fromisoformat(_day).weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {today_weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(_day)
        logging.info(query_res[0])
        logging.info(query_res)

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_art,
        op_args=['{{ ds }}'])

    # Устанавливаем зависимости между операторами
    start_task >> get_data