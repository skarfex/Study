"""
Тестовый даг
"""
import airflow
#import jinja2
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator


DEFAULT_ARGS = {
    'start_date': days_ago(0,0,0,0,0),
    'owner': 'ann-avdizhiian',
    'poke_interval': 600
}


with DAG(dag_id="a-avdizhiyan-9-L4-V2",
    schedule_interval='*/1 * * * *',
    default_args=DEFAULT_ARGS,
    max_active_runs=3,
    tags=['annicious','L4']
) as dag:


    dummy = DummyOperator(task_id="dummy")

    echo_ = BashOperator(
        task_id='echo_annicious',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag
    )
    def hi_func():
        logging.info("hi hi hi hi hi hi hi hi hi hi hi hi hi")

    hi = PythonOperator(
        task_id='hi_COMMON',
        python_callable=hi_func,
        dag=dag
    )
    def implicit_push_func():
        var = 'implicit_push'
        logging.info("Hello World! WOW WOW WOW WOW WOW")
        return var


    implicit_push = PythonOperator(
        task_id='implicit_push_id',
        python_callable=implicit_push_func,
        dag=dag
    )

    def implicit_pull_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['templates_dict']['implicit'])
        logging.info('--------------')

    implicit_pull = PythonOperator(
        task_id='implicit_pull_task_id',
        python_callable=implicit_pull_func,
        dag=dag,
        templates_dict = {'implicit': '{{ ti.xcom_pull(task_ids="implicit_push_id") }}'},
        provide_context=True
    )

    def explicit_push_func(**kwargs):
        kwargs['ti'].xcom_push(value='Hello world!', key='hi')
        kwargs['ti'].xcom_push(value='Value2!', key='va2')

    explicit_push = PythonOperator(
        task_id='explicit_push_id',
        python_callable=explicit_push_func,
        dag=dag,
        provide_context=True
    )

    def explicit_pull_func(**kwargs):
        print('--------------')
        print(kwargs['ti'].xcom_pull(task_ids='explicit_push_id', key='hi'))
        print(kwargs['ti'].xcom_pull(task_ids='explicit_push_id', key='va2'))

    explicit_pull = PythonOperator(
        task_id='explicit_pull_id',
        python_callable=explicit_pull_func,
        dag=dag,
        provide_context=True
    )
    def other_explicit_pull_func(**kwargs):
        logging.info('--------------')
        logging.info(kwargs['ti'].xcom_pull(task_ids='explicit_push_id', key='hi'))
        logging.info(kwargs['ti'].xcom_pull(task_ids='explicit_push_id', key='va2'))



    other_explicit_pull=PythonOperator(
        task_id='other_explicit_pull_id',
        python_callable=other_explicit_pull_func,
        dag=dag,
        provide_context=True
    )
    def is_NOTweekend_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        logging.info(execution_dt)
        return exec_day in [1,2,3,4,5]

    workdays_only = ShortCircuitOperator(
        task_id='workdays_only',
        python_callable=is_NOTweekend_func,
        op_kwargs={'execution_dt':'{{ds}}'} ##2022-08-26
    )



     #some_task = DummyOperator(task_id='some_task')


    dummy >> echo_ >> workdays_only >> hi >> implicit_push >> implicit_pull
    dummy >> explicit_push >> explicit_pull >> other_explicit_pull



