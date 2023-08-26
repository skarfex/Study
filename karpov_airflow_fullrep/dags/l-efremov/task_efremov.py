"""
Леонид Ефремов
Задание к уроку 4. Сложные пайплайны.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook


DEFAULT_ARGS = {
    'owner': 'l-efremov',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 3, 1)
}


def not_sunday_func(execution_dt, **kwargs):
    exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
    kwargs['ti'].xcom_push(value=exec_day+1, key='week_day')  # 1 = Monday
    return exec_day not in [6]


def gp_collect_func(**kwargs):
    """Connect to Greenplum server karpovcourses"""

    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("data_collector")
    s_id = kwargs['ti'].xcom_pull(task_ids='not_sunday', key='week_day')
    cursor.execute("SELECT heading FROM public.articles WHERE id={idx}"\
                   .format(idx=s_id))
    query_res = cursor.fetchall()
    logging.info(query_res)


with DAG(
        dag_id="ds_task1",
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        schedule_interval='@daily',
        tags=['l-efremov'],
        catchup=False
) as dag:
    begin = DummyOperator(task_id='init')

    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=not_sunday_func,
        op_kwargs={'execution_dt': '{{ds}}'},
        provide_context=True
    )

    gp_collect = PythonOperator(
        task_id='load_from_gp',
        python_callable=gp_collect_func,
        provide_context=True
    )

    begin >> not_sunday >> gp_collect
