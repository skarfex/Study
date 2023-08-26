"""
Task from Third Lesson: Complicated pipelines
"""
from datetime import datetime,timedelta
#from airflow import DAG
from airflow.decorators import dag
from airflow.utils.dates import days_ago
import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    'owner': 'e-safiulin',
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'poke_interval': 600
}

@dag(
    dag_id='e-safiulin_four_lesson_part_two',
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['karpov_courses','e-safiulin']
)
def dag_generator():
    """
    Dag printed heading of articles filtered by weekday 
    """

    start_message = DummyOperator(
        task_id = 'StartDAG'
    )

    def get_data_from_postgres(date:str) -> None:
        get_id = datetime.strptime(date, '%Y-%m-%d').isoweekday()
        p_hook = PostgresHook('conn_greenplum')
        conn = p_hook.get_conn()
        curr = conn.cursor('cursor_name')
        curr.execute(f"""
        SELECT heading from public.articles where id = {get_id}
        """)
        query_result = curr.fetchone()
        logging.info(query_result[0])

    execute_from_postgres = PythonOperator(
        task_id = 'Get_data_from_GP',
        python_callable = get_data_from_postgres,
        op_kwargs = dict(date = '{{ ds }}')
    )

    start_message >> execute_from_postgres

    dag.doc_md = __doc__
    execute_from_postgres.doc_md = """Извлекает данные из ГП и печатает их в лог"""

DAGGY = dag_generator()
