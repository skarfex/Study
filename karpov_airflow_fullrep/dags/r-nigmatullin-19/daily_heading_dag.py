"""
Это даг, который логирует heading из таблицы articles за заданный день
Он состоит из питон-оператора для выбора опции по дате,
и питон-оператора с pg hook, который выгружает данные.
"""

from airflow import DAG
import logging
from textwrap import dedent

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),
    'end_date': datetime(2022, 3, 14),
    'owner': 'r-nigmatullin-19'
}

with DAG("rmnigm_daily_article_dag",
         schedule_interval='0 2 * * 1-6',
         default_args=DEFAULT_ARGS,
         max_active_runs=1,
         tags=['r-nigmatullin-19']) as dag:
    
    def get_today_article_func(**kwargs):
        today = kwargs['templates_dict']['today']
        day_of_week = datetime.strptime(today, '%Y-%m-%d').isoweekday()
        logging.info(f'Day of week: {day_of_week}')
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        cursor = pg_hook.get_conn().cursor("named_cursor_name")
        query = f'SELECT heading FROM articles WHERE id = {day_of_week}'
        cursor.execute(query)
        result = cursor.fetchall()[0][0]
        log_text = dedent(f"""
                    -----------------------------------
                    Execution date: {today}
                    Day of week: {day_of_week}
                    Resulting heading: {result}
                    """)
        logging.info(log_text)


    get_string_for_today = PythonOperator(
        task_id='get_string_for_today',
        python_callable=get_today_article_func,
        templates_dict={'today': '{{ ds }}'}
    )