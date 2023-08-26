from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from datetime import timedelta


from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'm-borodastov',
    'poke_interval': 600
}

with DAG(
    dag_id='m-borodastov_lesson_3',
    schedule_interval='@hourly',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['m-borodastov']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    wait_1min = TimeDeltaSensor(
        task_id='wait_1min',
        delta=timedelta(seconds=60)
    )

    wait_2min = TimeDeltaSensor(
        task_id='wait_2min',
        delta=timedelta(seconds=2*60)
    )


    def first_task():
        logging.info("first task run")

    task1 = PythonOperator(
        task_id='task1',
        python_callable=first_task,
        dag=dag
    )

    def second_task():
        logging.info("second_task run")

    task2 = PythonOperator(
        task_id='task2',
        python_callable=second_task,
        dag=dag
    )


    # def is_weekend_func(execution_dt):
    #     exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    #     return exec_day in [5, 6]
    #
    #
    # weekend_only = ShortCircuitOperator(
    # task_id='weekend_only',
    # python_callable=is_weekend_func,
    # op_kwargs={'execution_dt': '{{ ds }}'}
    # )

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    dict_days = {0 : 'task_monday',
                 1 : 'task_tuesday',
                 2 : 'task_wednesday',
                 3 : 'task_thursday',
                 4 : 'task_friday',
                 5 : 'task_saturday',
                 6 : 'task_sunday'}
    def select_route_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        return dict_days[exec_day]


    select_route = BranchPythonOperator(
        task_id='select_route',
        python_callable=select_route_func,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )

    task_monday = DummyOperator(task_id='task_monday')
    task_tuesday = DummyOperator(task_id='task_tuesday')
    task_wednesday = DummyOperator(task_id='task_wednesday')
    task_thursday = DummyOperator(task_id='task_thursday')
    task_friday = DummyOperator(task_id='task_friday')
    task_saturday = DummyOperator(task_id='task_saturday')
    task_sunday = DummyOperator(task_id='task_sunday')



    dummy >> task1 >> wait_1min >> task2 >> wait_2min >> echo_ds >> select_route >> [
        task_monday, task_tuesday, task_wednesday, task_thursday, task_friday, task_saturday, task_sunday ]