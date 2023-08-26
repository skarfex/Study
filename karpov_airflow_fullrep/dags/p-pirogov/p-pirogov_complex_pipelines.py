"""
Создаю Даг для оправки на проверку по заданию курса СЛОЖНЫЕ ПАЙПЛАЙНЫ, ЧАСТЬ 1
Включает:
— DummyOperator (start и end)
— BashOperator с выводом даты (print_bash)
— PythonOperator с выводом даты (print_python)

Добавил проверку на чётность дня

"""
from airflow import DAG
from datetime import datetime
import logging
from textwrap import dedent
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {

    'owner': 'p-pirogov',
    #'email': ['pppirogov21@gmail.com'],
    'email_on_failure': True,
    'poke_interval': 60,
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15)
}
with DAG(
        dag_id='p-pirogov_complex_pipelines.py',
        schedule_interval='0 0 * * 1-6',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        tags=['p-pirogov']
) as dag:
    start = DummyOperator(task_id='start')

    template_str = dedent("""
    ______________________________________________________________
    
    ds: {{ ds }}
    ds_nodash: {{ ds_nodash }}
    ts: {{ts}}
    
    ______________________________________________________________
    """)


    def print_python_func(print_info):
        logging.info(print_info)


    print_python = PythonOperator(
        task_id='print_python',
        python_callable=print_python_func,
        op_args=[template_str]
    )

    print_bash = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ macros.ds_add(ds, -10) }}'
    )

    start >> [print_bash, print_python]


    def select_day_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = (datetime.strptime(execution_dt, '%Y-%m-%d'))
        a = exec_day.day
        return 'even' if a % 2 == 0 else 'not_even'


    even_or_not_even = BranchPythonOperator(
        task_id='even_or_not_even',
        python_callable=select_day_func,
        templates_dict={'execution_dt': '{{ ds }}'}

    )


    def even_func():
        logging.info("It's even day")


    even = PythonOperator(
        task_id='even',
        python_callable=even_func

    )


    def not_even_func():
        logging.info("It's not_even")


    not_even = PythonOperator(
        task_id='not_even',
        python_callable=not_even_func

    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )


    def load_gp_func(**kwargs):

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        execution_dt = kwargs['templates_dict']['execution_dt']
        b = datetime.strptime(execution_dt, "%Y-%m-%d").date()
        day = datetime.isoweekday(b)
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        #one_string = cursor.fetchone()[0]  # если вернулось единственное значение
        logging.info("______________________________________________________________")
        logging.info(query_res)
        logging.info(execution_dt)
        logging.info("______________________________________________________________")
        #logging.info(one_string)

    load_gp_func = PythonOperator(
        task_id='load_gp_func',
        python_callable=load_gp_func,
        templates_dict={'execution_dt': '{{ ds }}'}

    )

    print_python >> load_gp_func >> even_or_not_even >> [even, not_even]  >>end
