'''
Простейший DAG для урока-3

Схема:
— DummyOperator
— BashOperator с выводом даты
— PythonOperator с выводом даты
'''

import logging

from airflow import DAG
from airflow.utils.dates import days_ago
# from airflow.operators.dummy import EmptyOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.decorators import dag


DEFAULT_ARGS = {
    'start_date': days_ago(3),
    'owner': 's-halikov',
    'poke_interval': 300
}

with DAG(
    'simple_dag',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['simple', 'first', 'salavat']
) as dag:

    dummy = DummyOperator(task_id='dummy')

    echo_dt = BashOperator(
        task_id='echo_date',
        bash_command='echo {{ ds }}'
    )

    def print_date_func(arg1, arg2, **kwargs):
        logging.info('------------------------')
        # op_args
        logging.info(f'op_args: №1: {arg1}, №2: {arg2}')
        logging.info(f'kwarg.name: {kwargs["name"]}\nkwarg.sex: {kwargs["sex"]}')
        # templates
        logging.info('templates_dict, task_owner: ' + kwargs['templates_dict']['task_owner'])
        # context
        logging.info('context, {{ ds }}: ' + kwargs['ds'])
        logging.info('context, {{ tomorrow_ds }}: ' + kwargs['tomorrow_ds'])
        logging.info('------------------------')


    print_date = PythonOperator(
        task_id='print_date',
        python_callable=print_date_func,
        op_args=['arg1', 'arg2'],
        op_kwargs={'name': 'Salavat',
                   'sex': 'male'},
        templates_dict={'task_owner': '{{ task.owner }}'},
        provide_context=True
    )

    dag.doc_md = __doc__
    dummy.doc_md = '''Dummy оператор. В данном контексте ничего не делает.'''
    echo_dt.doc_md = '''Dummy оператор. Показывать дату ds'''
    print_date.doc_md = '''Выводит переданные аргументы и аргументы из контекста'''

    dummy >> echo_dt >> print_date

