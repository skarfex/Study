from airflow import DAG
import logging
from airflow.utils.dates import days_ago
from datetime import timedelta

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 15),
    'email': ['mandrusova@mail.ru'],
    'email_on_failure': True,
    'poke_interval': 30,
    'owner': 'z-mandrusova'
}

with DAG("zm_test",
         schedule_interval='0 0 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['zm']
         ) as dag:


    def get_data_from_gp_func(week_day, **kwargs):
        day_of_week = datetime.strptime(week_day, '%Y-%m-%d').weekday() + 1
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("named_cursor_name")
        cursor.execute(f"SELECT heading FROM articles where id = {day_of_week}")
        query_res = cursor.fetchall()
        logging.info(query_res)


    get_data_from_gp = PythonOperator(
        task_id='get_data_from_gp',
        python_callable=get_data_from_gp_func,
        dag=dag,
        provide_context=True,
        op_kwargs={'week_day': "{{ ds }}"}
    )

    get_data_from_gp
