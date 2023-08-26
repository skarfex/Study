import logging
import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from airflow.exceptions import AirflowSkipException

DEFAULT_ARGS = {
    'owner': 'v-ivanchenko-11',
}

with DAG('v-ivanchenko-11-print-from-gp',
         schedule_interval='@daily',
         start_date=datetime.datetime(2022, 3, 2),
         end_date=datetime.datetime(2022, 3, 15),
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         catchup=True,
         tags=['vi_print_from_gp']
         ) as dag:

    def print_select_from_greenplum(**context):

        day_number = datetime.datetime.fromisoformat(context['ds']).weekday() + 1

        if day_number != 7:

            pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
            conn = pg_hook.get_conn()
            cursor = conn.cursor("cursor_select_from_gp")
            cursor.execute(f'SELECT heading FROM articles WHERE id = {day_number}')
            query_res = cursor.fetchall()
            # one_string = cursor.fetchone()[0]
            cursor.close()
            logging.info(query_res)

        else:
             raise AirflowSkipException

    print_select_for_day = PythonOperator(
        task_id='print_select_from_gp',
        python_callable=print_select_from_greenplum,
        provide_context=True,
        dag=dag
    )

    print_select_for_day

