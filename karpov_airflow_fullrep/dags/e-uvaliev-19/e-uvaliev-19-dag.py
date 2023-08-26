"""
Даг по Уроку 3
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from textwrap import dedent
import pendulum
import logging


from airflow.operators.bash import BashOperator
#from airflow.operators.python.pythonoperator import PythonOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': pendulum.today('UTC').add(days=-2),
    'owner': 'e-uvaliev-19',
    'poke_interval': 600
    
}

with DAG("e-uvaliev-19-dag",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-uvaliev-19']
    ) as dag:


    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )


    template_str = dedent ("""
    ds: {{ ds }}
    """)

    def print_template(print_this):
        logging.info(print_this)

    print_t = PythonOperator(
        task_id='print_t',
        python_callable=print_template,
        op_args=[template_str],
        dag=dag
    )

    echo_ds >> print_t