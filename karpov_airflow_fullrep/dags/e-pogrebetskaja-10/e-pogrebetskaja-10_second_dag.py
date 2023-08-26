"""
Даг, выводящий heading статьи с id, равным дню недели ds.
Даг состоит из 2 операторов:
 - bash-оператора (выводит дату)
 - python-оператора (выводит heading статьи с id, равным дню недели ds)
"""

from airflow import DAG
import logging
from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-pogrebetskaja-10',
    'poke_interval': 600
}

with DAG("lenush_second_dag",
          schedule_interval='0 0 * * 1-6',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=['e-pogrebetskaja-10']
         ) as dag:

    dummy = DummyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    def read_heading_func(execution_dt):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()+1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute('SELECT heading FROM articles WHERE id = {}'.format(exec_day))
        query_res = cursor.fetchall()
        return query_res


    read_heading = PythonOperator(
        task_id='read_heading',
        python_callable=read_heading_func,
        op_kwargs={'execution_dt': '{{ ds }}'},
        dag=dag
    )

    dummy >> [echo_ds, read_heading]

    dag.doc_md = __doc__

    echo_ds.doc_md = """Пишет в лог execution_date"""
    read_heading.doc_md = """Пишет heading"""
