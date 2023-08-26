from airflow import DAG
# from datetime import datetime
import logging
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': '2022-03-01',
    'end_date': '2022-03-14',
    'owner': 'l-potsabej',
    'poke_interval': 600
}

with DAG(
    dag_id="l-potsabej_dag",
    schedule_interval='0 0 * * 1-6',
    max_active_runs=1,
    tags=['l-potsabej', 'l-potsabej_dag'],
    default_args=DEFAULT_ARGS
) as dag_launches:

    dummy = DummyOperator(
        task_id="start_operator"
    )

    # date_filter = execution_date.isoweekday()

    # print_heading_from_articles_pg = PostgresOperator(
    #     task_id="print_heading_from_articles_pg",
    #     postgres_conn_id="conn_greenplum",
    #     sql="SELECT heading FROM public.articles WHERE id = %(day_week)s",
    #     parameters={"day_week": {{ execution_date.isoweekday() + 1}}},
    # )

    def select_heading_from_articles_func(weekday):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # инициализируем хук
        conn = pg_hook.get_conn()  # берём из него соединение
        cursor = conn.cursor("named_cursor_name")  # и именованный (необязательно) курсор
        cursor.execute(f'SELECT heading FROM articles WHERE id = {weekday}')  # исполняем sql
        query_res = cursor.fetchall()  # полный результат
        logging.info(query_res)


    select_heading_from_articles = PythonOperator(
        task_id='select_heading_from_articles',
        python_callable=select_heading_from_articles_func,
        op_args=['{{ execution_date.isoweekday() + 1}}']
    )

    notify = BashOperator(
            task_id="notify",
            bash_command='echo "Это end"'
        )

    dummy >> select_heading_from_articles >> notify
