import logging
from datetime import date, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'm-shabunja-14',
    'poke_interval': 600
}

with DAG('m-shabunja-14_dag_2',
         schedule_interval='0 0 * * 1-6',
         catchup=True,
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['m-shabunja-14']
         ) as dag:

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ds}}',
        dag=dag
    )

    def get_heading_for_today_func(**kwargs):
        today = kwargs['templates_dict']['today']
        day_of_week = datetime.strptime(today, '%Y-%m-%d').weekday() + 1

        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        cursor = pg_hook.get_conn().cursor("named_cursor_name")
        cursor.execute(f'SELECT heading FROM articles WHERE id = {day_of_week}')
        logging.info(cursor.fetchall()[0][0])

    get_heading_for_today = PythonOperator(
        task_id='get_heading_for_today',
        python_callable=get_heading_for_today_func,
        templates_dict={'today': '{{ ds }}'},
        dag=dag
    )

    echo_ds >> get_heading_for_today
