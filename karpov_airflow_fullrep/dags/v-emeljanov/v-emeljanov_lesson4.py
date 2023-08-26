"""
Урок 4 - сложный даг
"""
from airflow import DAG
# from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
# from airflow.models.xcom import XCom
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'v_emeljanov',
    'poke_interval': 600
}

with DAG(dag_id="v_emeljanov_lesson4",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['v-emeljanov']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")  # dummy operator

    date_bash = BashOperator(
        task_id='date_bash',
        bash_command='echo {{ ds }}',
        dag=dag
    )   # bash operator (current date example in bash)

    def select_day_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday() + 1
        kwargs['ti'].xcom_push(key='exec_day', value=str(exec_day))  # push weekday to another task
        logging.info('--------------------')
        logging.info('exec_day (pushed): ' + str(exec_day))
        logging.info('--------------------')
        return 'dummy_sunday' if exec_day == 7 else 'read_gp_py'    # return dummy_sunday task for sunday only

    not_sunday = BranchPythonOperator(
        task_id='not_sunday',
        python_callable=select_day_func,
        templates_dict={'execution_dt': '{{ ds }}'},
        dag=dag
    )   # branch operator to select next task depending on weekday

    def read_gp_func(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')  # init hook
        conn = pg_hook.get_conn()  # get connection from the hook
        cursor = conn.cursor("named_cursor_name")  # get named (optional) cursor
        exec_day = kwargs['ti'].xcom_pull(task_ids='not_sunday', key='exec_day')    # pull weekday from xcom
        cursor.execute(f"SELECT heading FROM articles WHERE id = {exec_day}")   # exec sql
        query_res = cursor.fetchall()  # get all rows
        # one_string = cursor.fetchone()[0]  # get single row
        logging.info('--------------------')
        # log date and weekday:
        logging.info('ds: ' + kwargs['ds'])
        logging.info(f'exec_day(pulled): {exec_day}')
        # log results:
        for res in query_res:
            logging.info(f'Result: {res}')  # output by row
        logging.info('--------------------')

    read_gp_py = PythonOperator(
        task_id='read_gp_py',
        python_callable=read_gp_func,
        provide_context=True,
        dag=dag
    )   # python operator to extract data from GP

    date_bash >> not_sunday >> [read_gp_py, dummy]
