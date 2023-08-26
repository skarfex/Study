from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils.dates import days_ago
import logging

DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 'di-kudrjavtseva',
    'poke_interval': 3600
}

with DAG('di-kudrjavtseva_hw_les_3',
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['di-kudrjavtseva']
         ) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_ds = BashOperator(
        task_id = 'day_ds',
        bash_command='echo {{ ds }}',
        dag = dag
    )


    def wake_up_neo():
        logging.info('Wake up, Neo. The Matrix has you.')


    wake_up = PythonOperator(
        task_id='wake_up',
        python_callable=wake_up_neo,
        dag=dag
    )

    def print_ds(**kwargs):
        logging.info(kwargs['ds'])

    python_ds = PythonOperator(task_id='python_ds',
                               python_callable=print_ds,
                               op_kwargs={'ds': '{{ ds }}'})

    dummy >> [echo_ds, wake_up, python_ds]