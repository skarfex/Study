from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(5),
    'owner': 'r-sarychev',
    'poke_interval': 120
}

with DAG('r-sarychev_dag_hw_m3_l3_t0',
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=10,
          tags=['sarychev']
          ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo "Run echo_ds from r-sarychev_dag_hw_m3_l3_t0"',
        dag=dag
    )

    def hello_world_func():
        logging.info("Run hello_world_func from r-sarychev_dag_hw_m3_l3_t0")


    hello_world = PythonOperator(
        task_id='first_task',
        python_callable=hello_world_func,
        dag=dag
    )


    dummy >> [echo_ds, hello_world]
