from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': datetime (2022, 3, 1),
    'end_date': datetime (2022, 3, 14),
    'owner': 'm-goldin-20',
    'poke_interval': 600
}

with DAG("m-goldin-20-lesson-3_new",
         schedule_interval='0 9 * * MON,TUE,WED,THU,FRI,SAT',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-goldin-20']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    def select_day_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        logging.info(exec_day)
        return exec_day

    def data_GP(execute):
        pg_hook = PostgresHook('conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        date_format = '%Y-%m-%d'
        date_execute = datetime.strptime(execute, date_format).date()
        date_of_week_execute = date_execute.isoweekday()
        query_sql = (f""" SELECT heading from public.articles where id = {date_of_week_execute}""")
        cursor.execute(query_sql)
        result = cursor.fetchall()
        logging.info(result)

        cursor.close()
        conn.close

    start = PythonOperator(
        task_id='start',
        python_callable=data_GP,
        op_kwargs={'execute': '{{ ds }}'}
    )


    dummy >> start

