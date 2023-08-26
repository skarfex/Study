"""
Dag for thirth lesson. 
It consists of 3 operators, which are print text.
We use BashOperator, DummyOperator and PythonOperator.
"""
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'a-gunicheva-16',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 4)

}

dag = DAG("a-gunicheva-16-lesson-3",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['a-gunicheva-16']
          )


hello_bash = BashOperator(
    task_id="hello_bash",
    bash_command='echo "hello_bash"',
    dag=dag
)

def hello(**kwargs):
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))


python_hello = PythonOperator(
    task_id='python_hello',
    dag=dag,
    python_callable=hello,
    op_kwargs={'my_keyword': 'Airflow'}
)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

hello_bash >> dummy_task >> python_hello
