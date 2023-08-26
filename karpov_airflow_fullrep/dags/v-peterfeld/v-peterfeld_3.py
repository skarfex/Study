"""
Тестовый даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from textwrap import dedent

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'v-peterfeld',
    'poke_interval': 600
}

with DAG("v-peterfeld_3",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['v-peterfeld','task_3']) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}')


    def first_task(x):
        logging.info("Hello peterfeld, good work")
        logging.info(x)


    template_str = dedent("""
    ______________________________________________________________
    ds: {{ ds }}
    ds_nodash: {{ ds_nodash }}
    ts: {{ts}}
    ______________________________________________________________
    """)

    my_first_task = PythonOperator(
        task_id='my_first_task',
        python_callable=first_task,
        op_args=[template_str]
    )

dummy >> [echo_ds, my_first_task]

dag.doc_md = __doc__
echo_ds.doc_md = """Пишет в лог execution_date"""
my_first_task.doc_md = """Делает Принт строки + несколько строк из шаблонов jinja"""
dummy.doc_md = """Задает возможность сгруппировать таски"""