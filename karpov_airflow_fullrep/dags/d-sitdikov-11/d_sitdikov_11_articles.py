"""
Вывод заголовка статьи по дню недели
"""


from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator

my_name = 'd-sitdikov-11'

DEFAULT_ARGS = {
    'start_date': days_ago(156),
    'owner': my_name,
    'poke_interval': 600
}

dag = DAG("d_sitdikov_11_articles",
          schedule_interval='0 0 * * MON-SAT',
          default_args=DEFAULT_ARGS,
          max_active_runs=1,
          tags=[my_name]
          )


def find_date_func(**kwargs):
    current_date = kwargs['templates_dict']['current_date']
    current_day_int = datetime.strptime(current_date, '%Y-%m-%d').weekday() + 1
    return current_day_int

find_date = PythonOperator(
    task_id='find_date',
    python_callable=find_date_func,
    templates_dict={'current_date': '{{ ds }}'},
    dag=dag
)


def get_heading_func(current_day, **context):
    pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
    conn = pg_hook.get_conn()
    cursor = conn.cursor("fetch_headings_cursor")
    cursor.execute('SELECT heading FROM articles WHERE id = {0}'.format(current_day))
    heading = cursor.fetchone()[0]
    context['ti'].xcom_push(value=heading, key='target_heading')


get_heading = PythonOperator(
    task_id='get_heading',
    python_callable=get_heading_func,
    op_kwargs={"current_day": "{{ti.xcom_pull('find_date')}}"},
    provide_context=True,
    dag=dag
)

find_date >> get_heading

find_date.doc_md = """Возвращает текущий день недели в формате Понедельник - 0, Вторник - 1, ..."""
get_heading.doc_md = """Возвращает заголовок в текущий день недели"""
