"""
Простейший даг.
Состоит из думми-оператора,
баш-оператора (выводит дату),
питон-оператора (выводит дату)
"""

from airflow import DAG
from airflow.utils.dates import days_ago
import logging
# import datetime as dt

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 's-bayandina',
    'poke_interval': 600
}

dag = DAG("s-bayandina_dag_3_1_v1",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['s-bayandina']
          )

dummy=DummyOperator(task_id='dummy',
                    dag=dag
                    )

bash_date = BashOperator(task_id='bash_date',
                        dag=dag,
                        bash_command='echo {{ ds }}'
                        )


def get_today_date_func(**kwargs):
    logging.info(kwargs['date'])


python_date = PythonOperator(task_id='python_date',
                            python_callable=get_today_date_func,
                            dag=dag,
                            op_kwargs={'date': '{{ ds }}'}
                            )

dummy >> [bash_date, python_date]
