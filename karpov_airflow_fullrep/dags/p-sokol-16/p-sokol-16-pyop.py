from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG("dina_simple_dag_v2",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['karpov']
          )
wait_until_6am = TimeDeltaSensor(
    task_id='wait_until_6am',
    delta=timedelta(seconds=6 * 60 * 60),
    dag=dag
)


def hello(**kwargs):
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))
    t2 = PythonOperator(
        task_id='python_hello',
        dag=dag,
        python_callable=hello,
        op_kwargs={'my_keyword': 'Airflow'}
    )