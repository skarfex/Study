"""
Тестовый DAG Evgenii Safiulin
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
	'start_date': days_ago(1),
	'owner': 'e-safiulin',
	'poke_interval': 600
}

with DAG("esaf_test_dag",
	schedule_interval='@daily',
	default_args=DEFAULT_ARGS,
	max_active_runs=1,
	tags=['e-safiulin']
) as dag:

	dummy = DummyOperator(task_id="dummy")

	echo_ds = BashOperator(
		task_id='echo_ds',
		bash_command='echo {{ ds }}',
		dag=dag
	)

	def hello_world_func():
		logging.info("Hello World! from Esafi")

	hello_world = PythonOperator(
		task_id='print_hello',
		python_callable=hello_world_func,
		dag=dag
	)

	dummy >> [echo_ds, hello_world]
	
	dag.doc_md = __doc__
	echo_ds.doc_md = """Пишет в лог execution_date"""
	hello_world.doc_md = """Пишет 'Hello World! from Esafi' """
