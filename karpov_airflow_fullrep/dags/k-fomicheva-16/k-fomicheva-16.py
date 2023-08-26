"""
Простейший даг
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'k-fomicheva-16',
    'poke_interval': 500
}
with DAG(dag_id="ds_test_k-fomicheva-16",
    schedule_interval='0 0 * * 1-6',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['k-fomicheva-16']
) as dag:
    def get_string_for_today_func(**kwargs):
        this_date = kwargs['ds']
        day_of_week = datetime.strptime(this_date, '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        query_res = cursor.fetchall()
        result = query_res[0][0]
        return result

    get_string_for_today = PythonOperator(
        task_id='get_string_for_today',
        python_callable=get_string_for_today_func,
        provide_context=True,
        dag=dag
    )

    get_string_for_today