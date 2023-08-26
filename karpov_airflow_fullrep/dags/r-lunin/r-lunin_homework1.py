from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from datetime import date

from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.hooks.postgres_hook import PostgresHook


def my_log():
    logging.info("first_log")


def get_s_info_f_greenplum_1(day_of_week):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук # !!! обрати внимание на соединение c GP- оно задается через connection (BEST_pract_)
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    # day_of_week = datetime.datetime.today().weekday()+1
    cursor.execute(f"select heading from public.articles where id = {day_of_week}")  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(f'вот результат запроса: {query_res}')

def get_s_info_f_greenplum_2(day_of_week):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук # !!! обрати внимание на соединение c GP- оно задается через connection (BEST_pract_)
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    # day_of_week = datetime.datetime.today().weekday()+1
    cursor.execute(f"select heading from public.articles where id = {day_of_week}")  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(f'вот результат запроса: {query_res}')

def get_s_info_f_greenplum_3(day_of_week):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук # !!! обрати внимание на соединение c GP- оно задается через connection (BEST_pract_)
    conn = pg_hook.get_conn()  # берём из него соединение
    cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
    # day_of_week = datetime.datetime.today().weekday()+1
    cursor.execute(f"select heading from public.articles where id = {day_of_week}")  # исполняем sql
    query_res = cursor.fetchall()  # полный результат
    #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
    logging.info(f'вот результат запроса: {query_res}')



DEFAULT_ARGS = {
    'owner': 'lunin',
    'email': ['i@rlunin.ru'],
    'email_on_failure': True,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'start_date': days_ago(2),   #datetime(2022, 1, 1),
    # 'execution_timeout': timedelta(seconds=300),
    'trigger_rule':  'all_success'
}

with DAG(
    dag_id='R_lunin_dag',
    schedule_interval='10 12 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['lunin']
) as dag:

    start = DummyOperator(
        task_id='start',
        trigger_rule='dummy'
    )

    my_BashOperator = BashOperator(
        task_id='my_BashOperator',
        bash_command="echo {{ ds }}" #,
        #dag=dag
    )

    my_python_task = PythonOperator(
        task_id='my_python_task',
        python_callable = my_log,
        trigger_rule='all_success'
    )

    get_s_info_f_greenplum_1_task = PythonOperator(
        task_id='get_s_info_f_greenplum_1_task',
        python_callable = get_s_info_f_greenplum_1,
        op_args = '{{macros.datetime(2022, 3, 1).weekday()+1}}',
        trigger_rule='all_success'
    )

    get_s_info_f_greenplum_2_task = PythonOperator(
        task_id='get_s_info_f_greenplum_2_task',
        python_callable = get_s_info_f_greenplum_2,
        op_args = '{{macros.datetime(2022, 3, 6).weekday()+1}}',
        trigger_rule='all_success'
    )

    get_s_info_f_greenplum_3_task = PythonOperator(
        task_id='get_s_info_f_greenplum_3_task',
        python_callable = get_s_info_f_greenplum_3,
        op_args = '{{macros.datetime(2022, 3, 11).weekday()+1}}',
        trigger_rule='all_success'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_success'
    )

    # wait_until_6am = TimeDeltaSensor(
    #     task_id='wait_until_6am',
    #     delta=timedelta(seconds=6 * 60 * 60)
    # )
    #
start >> my_BashOperator >> my_python_task >> get_s_info_f_greenplum_1_task >> get_s_info_f_greenplum_2_task >> get_s_info_f_greenplum_3_task >> end