"""
Урок 4 задание 1
a-hromova_41

"""
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022,3,1),
    'end_date': datetime(2022,3,14),
    'owner': 'a-hromova',
    'poke_interval': 600
}

with DAG("a-hromova_41",
         schedule_interval='0 0 * 3 MON-SAT',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-hromova']
         ) as dag:

    def bd_select_func(exec_day, **kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {exec_day}')   # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        kwargs['ti'].xcom_push(value=query_res, key='article_head')  # пушим результат в xcom

    bd_select = PythonOperator(
        task_id='bd_select',
        python_callable=bd_select_func,
        op_args=['{{ dag_run.logical_date.weekday() + 1 }}', ], # день недели текущего выполнения дага
        provide_context=True
    )

    end_dummy = DummyOperator(
        task_id='end_dummy'
    )

    bd_select >> end_dummy



