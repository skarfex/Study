from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from datetime import timedelta
import pendulum



from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1, tz='utc'),
    'end_date': pendulum.datetime(2022, 3, 14, tz='utc'),
    'owner': 'm-borodastov',
    'poke_interval': 600
}

with DAG(
    dag_id='m-borodastov_lesson_4_practice',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-borodastov']
) as dag:


    dummy_start = DummyOperator(task_id="dummy_start")

    wait_1min = TimeDeltaSensor(
        task_id='wait_1min',
        delta=timedelta(seconds=60)
    )

    def check_day_func(**kwargs):
        execution_dt = kwargs['templates_dict']['execution_dt']
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return 'task_sunday' if exec_day == 6 else 'task_working_day'

    #оператор ветвления
    check_week_day = BranchPythonOperator(
        task_id='check_week_day',
        python_callable=check_day_func,
        templates_dict={'execution_dt': '{{ ds }}'}
    )



    def log_sunday():
        logging.info("today is sunday")

    task_sunday = PythonOperator(
        task_id='task_sunday',
        python_callable=log_sunday
    )


    def get_heading_from_gp_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        heading_id = exec_day+1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f"SELECT heading FROM articles WHERE id = {heading_id}")
        query_res = cursor.fetchall()
        logging.info(query_res)
        conn.close


    task_working_day = PythonOperator(
        task_id='task_working_day',
        python_callable=get_heading_from_gp_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    dummy_eod = DummyOperator(
        task_id='dummy_eod',
        trigger_rule='one_success',
    )


    dummy_start >> wait_1min >> check_week_day >> [task_sunday, task_working_day] >> dummy_eod