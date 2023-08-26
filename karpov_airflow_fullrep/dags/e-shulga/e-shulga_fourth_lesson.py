"""
Даг для заданий 3 и 4
"""
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import date, datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'e-shulga',
    'poke_interval': 600
}

def return_day_of_week(execution_date):
    execution_dt = datetime.strptime(execution_date, '%Y-%m-%d')
    return execution_dt.weekday() + 1

def get_heading_by_weekday(day_of_week, **context):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    print('select heading from articles where id = ' + str(day_of_week))
    cursor.execute('select heading from articles where id = ' + str(day_of_week))
    one_string = cursor.fetchone()[0]
    context['ti'].xcom_push(value=one_string, key='heading')

with DAG("e-shulga_fourth_lesson",
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         schedule_interval="0 0 * * 1-6",
         tags=['e-shulga']
         ) as dag:

    day_of_week = PythonOperator(
        task_id='day_of_week',
        python_callable=return_day_of_week,
        op_kwargs={'execution_date': '{{ ds }}'},
        dag=dag
    )

    get_heading = PythonOperator(
        task_id='get_heading',
        python_callable=get_heading_by_weekday,
        op_kwargs={"day_of_week": "{{ti.xcom_pull('day_of_week')}}"},
        provide_context=True,
        dag=dag
    )

    day_of_week >> get_heading