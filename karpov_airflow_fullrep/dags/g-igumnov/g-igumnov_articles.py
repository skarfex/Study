"""
Dag для урока 4: сложные пайплайны, часть 3. Необходимо создать dag, который работает с понедельника по субботу, и не работает в воскресенье, и забирает данные
из таблицы acticles greenplum-базы значение поля heading
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import logging
import pendulum

DEFAULT_ARGS = {
    'start_date': pendulum.datetime(2022, 3, 1),
    'end_date': pendulum.datetime(2022, 3, 14),
    'owner': 'g-igumnov',
    'poke_interval': 600
}

with DAG("g-igumnov_articles",
         schedule_interval='@daily',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['iga']
         ) as dag:

    def is_sunday_func(execution_dt,**kwargs):
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()
        kwargs['ti'].xcom_push(value=exec_day, key='day_of_week')
        logging.info('--------------')
        logging.info("python_date_of_week: " + str(exec_day))
        logging.info('--------------')
        return exec_day not in [6]

    not_sunday = ShortCircuitOperator(
        task_id='not_sunday',
        python_callable=is_sunday_func,
        provide_context=True,
        op_kwargs={'execution_dt': '{{ ds }}'}
    )


    def fetch_from_gp(**kwargs):
        exec_day = kwargs['ti'].xcom_pull(task_ids='not_sunday', key='day_of_week') + 1
        sql_query = f"select heading from articles where id = {exec_day}"
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor("curs_or")
        cursor.execute(sql_query)
        query_res = cursor.fetchone()[0]
        logging.info('--------------')
        logging.info("xcom_val: " + str(kwargs['ti'].xcom_pull(task_ids='not_sunday', key='day_of_week')))
        logging.info("date_of_week: " + str(exec_day))
        logging.info(f"heading: {query_res}")
        logging.info('--------------')

    print_text = PythonOperator(
        task_id='print_text',
        python_callable=fetch_from_gp,
        provide_context=True
    )

    not_sunday >> print_text