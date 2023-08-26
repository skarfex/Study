from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta


DEFAULT_ARGS = {
    'owner': 'n-marshanskij-8',
    'start_date': days_ago(2),
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
    'priority_weight': 10,
    'execution_timeout': timedelta(seconds=300),
    'trigger_rule':  'all_success'
    }


dag = DAG('n-marshanskij-8-first',
          start_date = days_ago(2),
          tags=['nmarshanskij']
          )

First_one = BashOperator(
    bash_command = "pwd",
    dag = dag,
    task_id = 'First_one'
)

Second_one = BashOperator(
    bash_command = 'sleep 5',
    dag = dag,
    task_id = 'Second_one'
)

Second_one >> First_one
