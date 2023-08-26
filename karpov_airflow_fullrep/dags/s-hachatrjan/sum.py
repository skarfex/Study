
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-hachatrjan',
    'poke_interval': 60
}

with DAG("sum",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['s-hachatrjan']
) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func,
        dag=dag
    )
    def push_sum(*args, **kwargs):
        return kwargs['ti'].xcom_push(value=sum(args), key='hi')


    sum_operator = PythonOperator(
        dag=dag,
        python_callable=push_sum,
        task_id='sum_task',
        op_args=[1, 2, 3]
    )

    def print_the_answer(**kwargs):
        logging.info(str(kwargs['ti'].xcom_pull(task_ids='sum_task', key='hi')))


    logging_func = PythonOperator(
        dag=dag,
        python_callable = print_the_answer,
        task_id='logging',
        provide_context = True
    )

    dummy >> [echo_ds, hello_world] >> sum_operator >> logging_func