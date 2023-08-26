from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import pendulum


def print_dt():
    print("{{ ds }}")


DEFAULT_ARGS = {
    'start_date': pendulum.today('UTC').add(minutes=-1),
    'owner': 'a-kravchenko',
    'poke_interval': 600
}


with DAG("a-kravchenko-19-test3",
    schedule_interval = '@daily',
    default_args = DEFAULT_ARGS,
    max_active_runs = 1,
    tags = ['a-kravchenko']) as dag:
    start = DummyOperator(task_id='start', dag=dag)
    end = DummyOperator(task_id='end', dag=dag)
    python_op_test = PythonOperator(task_id='python_op_test', python_callable=print_dt)
    start >> python_op_test >> end
