"""
Вариант DAG через оператор with
"""
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from a_ryzhkov_18_plugins.func_task import return_execution_dt, pull_ds_weekday_num

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov-18',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'priority_weight': 10,
    'start_date': datetime(2022, 3, 2),
    'end_date': datetime(2022, 3, 15),
    'sla': timedelta(minutes=7),
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule': 'all_success',
    'schedule_interval': '0 0 * * 1-6'
}

with DAG(dag_id='a-ryzhkov-18_dag_by_with',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['a-ryzhkov-18']) as dag:

    return_ds_operator = PythonOperator(
        task_id='return_ds',
        python_callable=return_execution_dt,
        templates_dict={'execution_dt': '{{ ds }}'},
        provide_context=True
    )

    return_row_in_table_db = PythonOperator(
        task_id='return_row',
        python_callable=pull_ds_weekday_num,
        provide_context=True
    )

    return_ds_operator >> return_row_in_table_db
