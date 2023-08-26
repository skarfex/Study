"""
DAG для урока "Сложные пайплайны"
"""
from airflow import DAG, macros
from airflow.utils.dates import days_ago
import logging
from airflow.decorators import dag, task
from textwrap import dedent

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'e-usmanov',
    'poke_interval': 600
}


@dag(
    start_date=days_ago(12),
    dag_id='eusmanov_complex_dag',
    schedule_interval='0 0 * * 6-7',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['e-usmanov']
)
def generate_dag():

    dummy = DummyOperator(task_id="dummy")

    commands = """
        echo {{ ds }}
        echo {{ macros.datetime.now() }}
        echo {{ execution_date - macros.timedelta(days=5) }}
        echo {{ macros.ds_add(ds, -4) }}
        """

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command=commands
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func
    )

    template_str = dedent("""
    ______________________________________________________________
    ds: {{ ds }}
    ds_nodash: {{ ds_nodash }}
    ts: {{ts}}
    gv_karpov: {{ var.value.gv_karpov }}
    gv_karpov, course: {{ var.json.gv_karpov_json.course }}
    ______________________________________________________________
    """)

    def print_template_func(print_this):
        logging.info(print_this)

    print_templates = PythonOperator(
        task_id='print_templates',
        python_callable=print_template_func,
        op_args=[template_str],
        trigger_rule='none_failed'
    )

    dummy >> [echo_ds, hello_world] >> print_templates


dag = generate_dag()
