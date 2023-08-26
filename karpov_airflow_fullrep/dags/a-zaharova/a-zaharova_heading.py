from airflow import DAG

import pendulum
import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    "start_date": pendulum.datetime(2022, 3, 1, tz="UTC"),
    "end_date": pendulum.datetime(2022, 3, 14, tz="UTC"),
    'owner': 'a-zaharova',
    'poke_interval': 30
}

with DAG("a-zaharova_latest_version_dag",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-zaharova']
         ) as dag:
    dummy = DummyOperator(task_id="dummy")
    echo_a = BashOperator(
        task_id='echo_a',
        bash_command='date +"%d-%m-%y"'
    )

    def today_date_func():
        print(datetime.datetime.now().strftime("%d-%m-%y"))

    today_date = PythonOperator(
        task_id='today_date',
        python_callable=today_date_func
    )
    dummy >> [echo_a, today_date]

    def anti_sunday_func(execution_date, **kwargs):
        exec_day = datetime.datetime.strptime(execution_date, '%Y-%m-%d').weekday()
        kwargs['ti'].xcom_push(value=(exec_day+1), key="weekday")
        return exec_day != 6

    def select_heading_func(execution_date, **kwargs):
        weekday = kwargs['ti'].xcom_pull(task_ids='anti_sunday', key='weekday')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("cursor_date")  # и именованный (необязательно) курсор
        cursor.execute('SELECT heading FROM articles WHERE id = '+str(weekday))  # исполняем sql
        one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        print(execution_date, ': ', one_string)

    anti_sunday = ShortCircuitOperator(
        task_id='anti_sunday',
        python_callable=anti_sunday_func,
        op_kwargs={'execution_date': '{{ds}}'}
    )

    select_heading = PythonOperator(
        task_id='select_heading',
        python_callable=select_heading_func,
        op_kwargs={"execution_date": "{{ ds }}"}
    )

    dummy >> anti_sunday >> select_heading
