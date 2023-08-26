from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(1),
    'owner': 'm-goldin-20',
    'poke_interval': 600
}

<<<<<<< HEAD
with DAG("m_goldin_dag",
=======
with DAG("m-goldin-20-lesson-3",
>>>>>>> 8dbc88869 (asdasdasd)
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-goldin-20']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

   def execution_date (**kwargs):
        logging.info('execution_date - '+kwargs['ds'])

    dt = PythonOperator(
        task_id='dt',
        python_callable=execution_date
    )


    dummy >> [echo_ds,print_templates]
