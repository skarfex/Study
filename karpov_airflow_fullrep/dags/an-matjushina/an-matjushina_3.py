from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    "owner": "a.matjushina",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def simple_python_func():
    print(
        """
         .-\"""-.
        /       \\
        \\       /
 .-\"""-.-`.-.-.< _
/      _,-\ ()()_/:)
\     / ,  `     `|
 '-..-| \-.,___,  /
       \ `-.__/  /
        `-.__.-'`
    """
    )


with DAG(
    "simple_dag_matjushina",
    default_args=DEFAULT_ARGS,
    description="A simple DAG",
    start_date=datetime(2022, 11, 6),
    catchup=False,
    tags=["a.matjushina"],
) as dag:
    dag.doc_md = """
        It is just simple test DAG with one python task and one bash task
    """

    t1 = PythonOperator(
        task_id="python_task",
        depends_on_past=False,
        python_callable=simple_python_func,
    )

    t2 = BashOperator(
        task_id="bash_task", depends_on_past=False, bash_command='echo "$PWD"'
    )

    t1 >> t2
