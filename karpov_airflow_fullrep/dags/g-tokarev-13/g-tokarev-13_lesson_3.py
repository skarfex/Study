from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.decorators import task

import pendulum


with DAG(
    dag_id='g-tokarev-13_lesson_3',
    schedule_interval='@daily',
    tags=['g-tokarev-13'],
    max_active_runs=1,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
) as dag:

    echo_timestamp = BashOperator(
        task_id='echo_timestamp',
        bash_command='echo {{ ts }}',
    )
    start = DummyOperator(
        task_id='start',
    )
    is_friday = BranchDayOfWeekOperator(
        task_id='is_friday',
        follow_task_ids_if_true="friday",
        follow_task_ids_if_false="another_day",
        week_day='Friday'
    )

    @task(task_id='friday')
    def friday():
        print(
            '\n'.join(
                'It\'s Friday, Friday'
                'Gotta get down on Friday'
                'Everybody\'s lookin\' forward to the weekend, weekend'
            )
        )
    friday_task = friday()

    @task(task_id='another_day')
    def another_day():
        print('Just another day')
    another_day_task = another_day()

    start >> echo_timestamp >> is_friday >> [friday_task, another_day_task]
