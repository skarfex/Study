"""
a-ryzhkov-18 dag less_4
"""
from datetime import timedelta, datetime
from a_ryzhkov_18_plugins.func_task import return_weekday_func, get_row_in_db
from airflow.decorators import dag

DEFAULT_ARGS = {
    'owner': 'a-ryzhkov-18',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'priority_weight': 10,
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'sla': timedelta(minutes=7),
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule': 'all_success',
    'schedule_interval': '0 10 * * 1-6'
}


@dag(default_args=DEFAULT_ARGS,
     max_active_runs=1,
     tags=['a-ryzhkov-18']
     )
def a_ryzhkov_18_less_4():
    get_row_in_db(return_weekday_func())


a_ryzhkov_18_less_4_dag = a_ryzhkov_18_less_4()
