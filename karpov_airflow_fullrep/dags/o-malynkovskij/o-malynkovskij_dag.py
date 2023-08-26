"""
Пайплайн по загрузке данных из таблицы GreenPlum (articles)
Данные выводятся в логах
"""
import logging
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, date

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz="UTC"),
    'end_date': pendulum.datetime(2022, 3, 14,  tz="UTC"),
    'owner': 'o-malynkovskij'
}

with DAG("o-malynkovskij_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['o-malynkovskij']
) as dag:

    def load_table_from_gp_func(exec_day):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        exec_week_day = date.fromisoformat(exec_day).weekday() + 1
        cursor.execute(f'SELECT heading FROM articles WHERE id = { exec_week_day }')  # исполняем sql
        #query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение

        result = cursor.fetchall()

        logging.info(str(result))

    load_table_from_gp = PythonOperator(
        task_id='load_table_from_gp',
        python_callable=load_table_from_gp_func,
        op_args=['{{ ds }}' ],
        dag=dag
    )




