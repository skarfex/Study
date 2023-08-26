"""
ra-valiev, Модуль 4, DAG, Уроки 3-4

"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.xcom import XCom

from datetime import datetime


DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1, 3),
    'end_date': datetime(2022, 3, 15, 2, 59, 59),
    'owner': 'ra-valiev',
    'poke_interval': 600
}

with DAG('ra-valiev_heading_DAG',
    schedule_interval='00 19 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ds','ra-valiev'],
    
) as dag:
    
    dummy = DummyOperator(task_id='start_dummy_task')
    
    echo_yesterday_ds = BashOperator(
        task_id='yesterday_bash_task',
        bash_command='echo {{ yesterday_ds }}',
    )
    
    echo_ds = BashOperator(
        task_id='ds_bash_task',
        bash_command='echo {{ ds }}',
    )
    
    echo_tomorrow_ds = BashOperator(
        task_id='tomorrow_bash_task',
        bash_command='echo {{ tomorrow_ds }}'
    )
    
    def print_dag_args_func(*args, **kwargs):
        logging.info('***********************************')
        for i, arg in enumerate(args):
            logging.info(f'op_args, №{i+1}: {arg}')
        logging.info(f"op_kwarg, №1: {kwargs['kwarg1']}")
        logging.info(f"op_kwarg, №2: {kwargs['kwarg2']}")
        logging.info('templates_dict, task_owner: ' + kwargs['templates_dict']['task_owner'])
        logging.info('context, Дата в формате nodash: ' + kwargs['ds_nodash'])
        logging.info('context, Дата в формате ts: ' + kwargs['ts'])
        kwargs['ti'].xcom_push(value=kwargs['ts'], key='exec_date')
        logging.info('***********************************')
    
    print_dag_args = PythonOperator(
        task_id='print_dag_args',
        python_callable=print_dag_args_func,
        op_args=['arg1', 'arg2'],
        op_kwargs={'kwarg1': 'kwargument1', 'kwarg2': 'kwargument2'},
        templates_dict={'task_owner': '{{ task.owner }}'},
        provide_context=True   
    )
    
    
    def get_from_articles_greenplum_func(**kwargs):
        exec_date = kwargs['ti'].xcom_pull(task_ids='print_dag_args', key = 'exec_date')
        exec_date_formatted = datetime.strptime(exec_date[:10], "%Y-%m-%d")
        get_id = datetime.isoweekday(exec_date_formatted)
        logging.info('день недели: ' + str(get_id))
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        connection = pg_hook.get_conn()
        cursor = connection.cursor('greenplum_cursor')
        cursor.execute(f'SELECT heading FROM articles where id = {get_id}')   
        query_result = cursor.fetchall()
        logging.info(f'Значение heading из таблицы articles по id, равному дню недели: {query_result[0][0]}')
        kwargs['ti'].xcom_push(value=query_result[0][0], key='task_result')
    
    get_from_articles_greenplum = PythonOperator(
        task_id='get_from_articles_greenplum',
        python_callable=get_from_articles_greenplum_func,
    )
    
    end_of_dag = DummyOperator(
        task_id='end_of_dag',
        trigger_rule='one_success'    
    )
    
    dummy >> [echo_yesterday_ds, echo_ds, echo_tomorrow_ds] >> print_dag_args >> get_from_articles_greenplum >> end_of_dag