"""
Тестовый даг, который подскажет, если пора идти спать
"""
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'a-filatova-20',
    'poke_interval': 600
}

with DAG("fil_test",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['a-filatova-20']
) as dag:

    dummy = DummyOperator(task_id="empty")

    echo_ds = BashOperator(
        task_id='echo_fil',
        bash_command='echo \"Hi! Current time: \" {{ ts }}',
        dag=dag
    )

    def go_to_bed():
        cur_date_time = datetime.now()
        time1=cur_date_time.replace(hour=23, minute=0, second=0, microsecond=0)
        time2 = cur_date_time.replace(hour=7, minute=0, second=0, microsecond=0)
        if cur_date_time > time1 or cur_date_time < time2:
            print("Go to bed")
        else:
            print("Wake up!")

    bed = PythonOperator(
        task_id='go_to_bed',
        python_callable=go_to_bed,
        dag=dag
    )

    dummy >> [echo_ds, bed]
