"""
Task from Five Lesson: Develop own pipelines
"""
import logging

from datetime import datetime,timedelta
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from e_safiulin_plugins.e_safiulin_ram_location import EvgeniiSafiulinRamLocationOperator


DEFAULT_ARGS = {
	'owner': 'e-safiulin',
	'start_date': days_ago(2),
	'poke_interval': 600
}

@dag(
	dag_id='e-safiulin_five_lesson',
	schedule_interval='@daily',
	default_args=DEFAULT_ARGS,
	max_active_runs=1,
	tags=['karpov_courses','e-safiulin']
)
def dag_generator():
	"""
	Dag make a requests to api and put data in the tables 
	"""

	start_message = DummyOperator(
		task_id = 'StartDAG'
	)

	top_locations_oper = EvgeniiSafiulinRamLocationOperator(
		task_id = 'Top3LocationsByResidentsToGP'
	)

	start_message >> top_locations_oper

	dag.doc_md = __doc__
	top_locations_oper.doc_md = """
	Создает таблицу если ее нет,
	очищает таблицу,
	далее делает запрос,
	фильтрует по топ 3 по резидентам
	и вновь пушит в базу"""

DAGGY = dag_generator()
