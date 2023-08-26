"""
Первый, тестовый DAG
Он состоит из empty оператора
bash оператора (выводит дату)
python оператора (выводит так же дату)
"""

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import logging

DEFAULT_ARGS = {
    "start_date": days_ago(1),
    "owner": "d-milovanov-3",
    "poke_interval": 600
}

with DAG("d-milovanov-3_module4_lesson3",
         schedule_interval="@daily",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=["d-milovanov-3", "lesson-3"]
         ) as dag:

    dummy = DummyOperator(task_id="dummy")
    

    echo_ds = BashOperator(
        task_id="echo_ds",
        bash_command="echo {{ ds }}",
        dag=dag
    )


    def execution_date(ds):
        print(f"The execution date is {ds}")


    logging_ds = PythonOperator(
        task_id="print_ds",
        python_callable=execution_date,
        dag=dag,
    )

dummy >> echo_ds >> logging_ds


dag.doc_md = __doc__

dummy.doc_md = """Просто dummy оператор"""
echo_ds.doc_md = """Выводит в лог execution_date"""
logging_ds.doc_md = """Выводит в лог execution_date"""



