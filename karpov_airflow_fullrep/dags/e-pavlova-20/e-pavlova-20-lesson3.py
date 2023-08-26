"""
Урок 3
Сложные пайплайны ч. 1
Задания
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'e-pavlova-20',
    'poke_interval': 600
}

with DAG(
     dag_id="pavlova-hw-lesson3",
     schedule_interval='@daily',
     default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['e-pavlova-20']
) as dag:

    def no_sunday_func():
        exec_day = datetime.strptime('2022-03-11', '%Y-%m-%d').weekday()
        exec_day = exec_day + 1
        return exec_day != 7

    no_sunday = ShortCircuitOperator(
        task_id='no_sunday',
        python_callable=no_sunday_func,
        #op_kwargs=({'execution_dt': '2022-03-01'}),
        dag=dag
    )

    def get_data_from_gp_func():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        exec_day = datetime.strptime('2022-03-11', '%Y-%m-%d').weekday()
        exec_day = exec_day + 1
        logging.info(str(exec_day))
        cursor.execute('SELECT heading FROM articles WHERE id = ' + str(exec_day))  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info(str(query_res))

    get_data_from_gp = PythonOperator(
        task_id='get_data_from_gp',
        python_callable=get_data_from_gp_func,
        #op_kwargs=({'execution_dt': '2022-03-01'}),
        dag=dag
    )

    no_sunday >> get_data_from_gp