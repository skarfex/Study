from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import weekday
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date' : datetime(2022, 3, 14),
    'owner': 's-hachatrjan',
    'poke_interval': 60
}

with DAG("Get_data_dag",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-hachatrjan'],
    catchup=True
) as dag:

    def get_data_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = {week_day}.format(week_day=kwargs['templates_dict']['current_date'])'  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        return query_res, one_string

    get_data = PythonOperator(
        task_id = 'pull_data_task',
        templates_dict = {'current_date' : {{ds}}.weekday + 1},
        python_callable=get_data_func,
    )
