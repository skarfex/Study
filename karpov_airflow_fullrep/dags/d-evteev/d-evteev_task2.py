"""
Тестовый даг
"""
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
import logging
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def work_hours():
    tm = datetime.now().strftime('%H:%M')
    work_start_tm = datetime(12, 1, 3, 9).strftime('%H:%M')
    work_end_tm = datetime(12,1,3,18).strftime('%H:%M')
    wkd = datetime.now().weekday()
    if tm > work_start_tm and tm < work_end_tm and wkd < 5:
        return "ВСТАВАЙ РАБОТАТЬ ПОРА!!!"
    elif wkd < 5 and (tm < work_start_tm or tm > work_end_tm):
        return "РАБОЧЕЕ ВРЕМЯ ЗАКОНЧИЛОСЬ ОТДЫХАЙ!!!"
    elif wkd > 5:
        return "СЕГОДНЯ ВЫХОДНОЙ ОТДЫХАЙ"

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'd-evteev',
    'poke_interval': 600
}

with DAG("d-evteev_task2",
    schedule_interval='0 0 * * MON-SAT',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['d-evteev'],
    user_defined_macros = {'my_cust_func': work_hours()}
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ macros.ds_format(ds, "%Y-%m-%d", "%w") }}',
        dag=dag
    )

    work_time = BashOperator(
        task_id='work_time',
        bash_command='echo {{ my_cust_func }}',
        dag=dag
    )

    def hello_world_func(**kwargs):
        a = kwargs['ds']
        logging.info("Hello World! ", a)

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        provide_context=True,
        dag=dag
    )

    def select_articles_func(wkd):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor()  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {wkd}')  # исполняем sql
        query_res = cursor.fetchall()
        logging.info(query_res)

    select_articles = PythonOperator(
        task_id='select_articles',
        op_args=['{{macros.ds_format(ds, "%Y-%m-%d", "%w")}}'],
        python_callable=select_articles_func,
        dag=dag
    )

    def bye_world_func():
        a = 50 + 50
        logging.info("Bye bye World!! ", a)

    bye_world = PythonOperator(
        task_id='bye_world',
        python_callable=bye_world_func,
        dag=dag
    )


    dummy >> [echo_ds, hello_world] >> select_articles >> work_time >> bye_world