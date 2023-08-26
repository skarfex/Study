import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 3, 0, 0),
    'end_date': datetime(2022, 3, 14, 3, 0, 0),
    'owner': 'a-lazarev',

}


def branch_for_a_weekday(st_date):
    exec_date = datetime.strptime(st_date, '%Y-%m-%d').weekday()
    if exec_date != 6:
        return 'pg_data_reader'
    else:
        return 'sunday'


def pg_data_reader(load_date):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT heading from articles where id = extract(isodow from date '{load_date}')")
    result = cursor.fetchone()[0]
    logging.info(result)


def sunday_stamp():
    logging.info("It's Sunday, nothing to return")


with DAG('a-lazarev-dag_less_4',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-lazarev', 'lesson-4']
         ) as dag:

    check_the_date = BranchPythonOperator(
        task_id='branch_for_a_weekday',
        python_callable=branch_for_a_weekday,
        op_kwargs=dict(
            st_date='{{ ds }}'
        )
    )

    pg_reader = PythonOperator(
        task_id='pg_data_reader',
        python_callable=pg_data_reader,
        op_kwargs=dict(
            load_date='{{ ds }}'
        )
    )

    sunday_task = PythonOperator(
        task_id='sunday',
        python_callable=sunday_stamp
    )

    check_the_date >> [pg_reader, sunday_task]


